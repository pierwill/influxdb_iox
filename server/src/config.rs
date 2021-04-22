use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{Arc, RwLock},
};

use data_types::{
    database_rules::{DatabaseRules, WriterId},
    DatabaseName,
};
use object_store::{path::ObjectStorePath, ObjectStore};
use query::exec::Executor;

/// This module contains code for managing the configuration of the server.
use crate::{buffer::Buffer, db::Db, Error, JobRegistry, Result};
use observability_deps::tracing::{self, error, info, warn, Instrument};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) const DB_RULES_FILE_NAME: &str = "rules.pb";

/// The Config tracks the configuration of databases and their rules along
/// with host groups for replication. It is used as an in-memory structure
/// that can be loaded incrementally from object storage.
///
/// drain() should be called prior to drop to ensure termination
/// of background worker tasks. They will be cancelled on drop
/// but they are effectively "detached" at that point, and they may not
/// run to completion if the tokio runtime is dropped
#[derive(Debug)]
pub(crate) struct Config {
    shutdown: CancellationToken,
    jobs: Arc<JobRegistry>,
    state: RwLock<ConfigState>,
}

pub(crate) enum UpdateError<E> {
    Update(Error),
    Closure(E),
}

impl<E> From<Error> for UpdateError<E> {
    fn from(e: Error) -> Self {
        Self::Update(e)
    }
}

impl Config {
    pub(crate) fn new(jobs: Arc<JobRegistry>) -> Self {
        Self {
            shutdown: Default::default(),
            state: Default::default(),
            jobs,
        }
    }

    pub(crate) fn create_db(&self, rules: DatabaseRules) -> Result<CreateDatabaseHandle<'_>> {
        let mut state = self.state.write().expect("mutex poisoned");
        if state.reservations.contains(&rules.name) || state.databases.contains_key(&rules.name) {
            return Err(Error::DatabaseAlreadyExists {
                db_name: rules.name.to_string(),
            });
        }

        state.reservations.insert(rules.name.clone());
        Ok(CreateDatabaseHandle {
            rules: Some(rules),
            config: &self,
        })
    }

    pub(crate) fn db(&self, name: &DatabaseName<'_>) -> Option<Arc<Db>> {
        let state = self.state.read().expect("mutex poisoned");
        state.databases.get(name).map(|x| Arc::clone(&x.db))
    }

    pub(crate) fn db_names_sorted(&self) -> Vec<DatabaseName<'static>> {
        let state = self.state.read().expect("mutex poisoned");
        state.databases.keys().cloned().collect()
    }

    pub(crate) fn update_db_rules<F, E>(
        &self,
        db_name: &DatabaseName<'static>,
        update: F,
    ) -> std::result::Result<DatabaseRules, UpdateError<E>>
    where
        F: FnOnce(DatabaseRules) -> std::result::Result<DatabaseRules, E>,
    {
        let state = self.state.read().expect("mutex poisoned");
        let db_state = state
            .databases
            .get(db_name)
            .ok_or_else(|| Error::DatabaseNotFound {
                db_name: db_name.to_string(),
            })?;

        let mut rules = db_state.db.rules.write();
        *rules = update(rules.clone()).map_err(UpdateError::Closure)?;
        Ok(rules.clone())
    }

    pub(crate) fn remotes_sorted(&self) -> Vec<(WriterId, String)> {
        let state = self.state.read().expect("mutex poisoned");
        state.remotes.iter().map(|(&a, b)| (a, b.clone())).collect()
    }

    pub(crate) fn update_remote(&self, id: WriterId, addr: GRpcConnectionString) {
        let mut state = self.state.write().expect("mutex poisoned");
        state.remotes.insert(id, addr);
    }

    pub(crate) fn delete_remote(&self, id: WriterId) -> Option<GRpcConnectionString> {
        let mut state = self.state.write().expect("mutex poisoned");
        state.remotes.remove(&id)
    }

    fn commit(
        &self,
        rules: DatabaseRules,
        writer_id: WriterId,
        object_store: Arc<ObjectStore>,
        exec: Arc<Executor>,
    ) {
        let mut state = self.state.write().expect("mutex poisoned");
        let name = state
            .reservations
            .take(&rules.name)
            .expect("reservation doesn't exist");

        if self.shutdown.is_cancelled() {
            error!("server is shutting down");
            return;
        }

        let write_buffer = rules
            .write_buffer_config
            .as_ref()
            .map(|config| Buffer::new_with_config(writer_id, config));
        let db = Arc::new(Db::new(
            rules,
            server_id,
            object_store,
            exec,
            write_buffer,
            Arc::clone(&self.jobs),
        ));

        let shutdown = self.shutdown.child_token();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let name_captured = name.clone();

        let handle = Some(tokio::spawn(async move {
            db_captured
                .background_worker(shutdown_captured)
                .instrument(tracing::info_span!("db_worker", database=%name_captured))
                .await
        }));

        assert!(state
            .databases
            .insert(
                name,
                DatabaseState {
                    db,
                    handle,
                    shutdown
                }
            )
            .is_none())
    }

    fn rollback(&self, name: &DatabaseName<'static>) {
        let mut state = self.state.write().expect("mutex poisoned");
        state.reservations.remove(name);
    }

    /// Cancels and drains all background worker tasks
    pub(crate) async fn drain(&self) {
        info!("shutting down database background workers");

        // This will cancel all background child tasks
        self.shutdown.cancel();

        let handles: Vec<_> = self
            .state
            .write()
            .expect("mutex poisoned")
            .databases
            .iter_mut()
            .filter_map(|(_, v)| v.join())
            .collect();

        for handle in handles {
            let _ = handle.await;
        }

        info!("database background workers shutdown");
    }
}

pub fn object_store_path_for_database_config<P: ObjectStorePath>(
    root: &P,
    name: &DatabaseName<'_>,
) -> P {
    let mut path = root.clone();
    path.push_dir(name.to_string());
    path.set_file_name(DB_RULES_FILE_NAME);
    path
}

/// A gRPC connection string.
pub type GRpcConnectionString = String;

#[derive(Default, Debug)]
struct ConfigState {
    reservations: BTreeSet<DatabaseName<'static>>,
    databases: BTreeMap<DatabaseName<'static>, DatabaseState>,
    /// Map between remote IOx server IDs and management API connection strings.
    remotes: BTreeMap<WriterId, GRpcConnectionString>,
}

#[derive(Debug)]
struct DatabaseState {
    db: Arc<Db>,
    handle: Option<JoinHandle<()>>,
    shutdown: CancellationToken,
}

impl DatabaseState {
    fn join(&mut self) -> Option<JoinHandle<()>> {
        self.handle.take()
    }
}

impl Drop for DatabaseState {
    fn drop(&mut self) {
        if self.handle.is_some() {
            // Join should be called on `DatabaseState` prior to dropping, for example, by
            // calling drain() on the owning `Config`
            warn!("DatabaseState dropped without waiting for background task to complete");
            self.shutdown.cancel();
        }
    }
}

/// CreateDatabaseHandle is returned when a call is made to `create_db` on
/// the Config struct. The handle can be used to hold a reservation for the
/// database name. Calling `commit` on the handle will consume the struct
/// and move the database from reserved to being in the config.
///
/// The goal is to ensure that database names can be reserved with
/// minimal time holding a write lock on the config state. This allows
/// the caller (the server) to reserve the database name, persist its
/// configuration and then commit the change in-memory after it has been
/// persisted.
#[derive(Debug)]
pub(crate) struct CreateDatabaseHandle<'a> {
    /// Partial moves aren't supported on structures that implement Drop
    /// so use Option to allow taking DatabaseRules out in `commit`
    rules: Option<DatabaseRules>,
    config: &'a Config,
}

impl<'a> CreateDatabaseHandle<'a> {
    pub(crate) fn commit(
        mut self,
        server_id: NonZeroU32,
        object_store: Arc<ObjectStore>,
        exec: Arc<Executor>,
    ) {
        self.config
            .commit(self.rules.take().unwrap(), server_id, object_store, exec)
    }

    pub(crate) fn rules(&self) -> &DatabaseRules {
        self.rules.as_ref().unwrap()
    }
}

impl<'a> Drop for CreateDatabaseHandle<'a> {
    fn drop(&mut self) {
        if let Some(rules) = self.rules.take() {
            self.config.rollback(&rules.name)
        }
    }
}

#[cfg(test)]
mod test {
    use object_store::{memory::InMemory, ObjectStore, ObjectStoreApi};

    use super::*;

    #[tokio::test]
    async fn create_db() {
        let name = DatabaseName::new("foo").unwrap();
        let config = Config::new(Arc::new(JobRegistry::new()));
        let rules = DatabaseRules::new(name.clone());

        {
            let _db_reservation = config.create_db(rules.clone()).unwrap();
            let err = config.create_db(rules.clone()).unwrap_err();
            assert!(matches!(err, Error::DatabaseAlreadyExists { .. }));
        }

        let db_reservation = config.create_db(rules).unwrap();
        let server_id = NonZeroU32::new(1).unwrap();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let exec = Arc::new(Executor::new(1));
        db_reservation.commit(server_id, store, exec);
        assert!(config.db(&name).is_some());
        assert_eq!(config.db_names_sorted(), vec![name.clone()]);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(
            config
                .db(&name)
                .expect("expected database")
                .worker_iterations()
                > 0
        );

        config.drain().await
    }

    #[tokio::test]
    async fn test_db_drop() {
        let name = DatabaseName::new("foo").unwrap();
        let config = Config::new(Arc::new(JobRegistry::new()));
        let rules = DatabaseRules::new(name.clone());

        let db_reservation = config.create_db(rules).unwrap();
        let server_id = NonZeroU32::new(1).unwrap();
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let exec = Arc::new(Executor::new(1));
        db_reservation.commit(server_id, store, exec);

        let token = config
            .state
            .read()
            .expect("lock poisoned")
            .databases
            .get(&name)
            .unwrap()
            .shutdown
            .clone();

        // Drop config without calling drain
        std::mem::drop(config);

        // This should cancel the the background task
        assert!(token.is_cancelled());
    }

    #[test]
    fn object_store_path_for_database_config() {
        let storage = ObjectStore::new_in_memory(InMemory::new());
        let mut base_path = storage.new_path();
        base_path.push_dir("1");

        let name = DatabaseName::new("foo").unwrap();
        let rules_path = super::object_store_path_for_database_config(&base_path, &name);

        let mut expected_path = base_path;
        expected_path.push_dir("foo");
        expected_path.set_file_name("rules.pb");

        assert_eq!(rules_path, expected_path);
    }
}
