//! This module implements the `database` CLI command
use std::{fs::File, io::Read, path::PathBuf, str::FromStr};

use influxdb_iox_client::{
    connection::Builder,
    flight,
    format::QueryOutputFormat,
    management::{
        self, generated_types::*, CreateDatabaseError, GetDatabaseError, ListDatabaseError,
    },
    write::{self, WriteError},
};
use structopt::StructOpt;
use thiserror::Error;

mod catalog;
mod chunk;
mod partition;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error creating database: {0}")]
    CreateDatabaseError(#[from] CreateDatabaseError),

    #[error("Error getting database: {0}")]
    GetDatabaseError(#[from] GetDatabaseError),

    #[error("Error listing databases: {0}")]
    ListDatabaseError(#[from] ListDatabaseError),

    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),

    #[error("Error reading file {:?}: {}", file_name, source)]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[error("Error writing: {0}")]
    WriteError(#[from] WriteError),

    #[error("Error formatting: {0}")]
    FormattingError(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    #[error("Error in chunk subcommand: {0}")]
    Chunk(#[from] chunk::Error),

    #[error("Error in partition subcommand: {0}")]
    Partition(#[from] partition::Error),

    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Error in partition subcommand: {0}")]
    Catalog(#[from] catalog::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx databases
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// Create a new database
#[derive(Debug, StructOpt)]
struct Create {
    /// The name of the database
    name: String,

    /// A chunk of data within a partition that has been cold for writes for
    /// this many seconds will be frozen and compacted (moved to the read
    /// buffer) if the chunk is older than mutable_min_lifetime_seconds
    ///
    /// Represents the chunk transition open -> moving and closed -> moving
    #[structopt(long, default_value = "300")] // 5 minutes
    mutable_linger_seconds: u32,

    /// A chunk of data within a partition is guaranteed to remain mutable
    /// for at least this number of seconds
    #[structopt(long, default_value = "0")] // 0 minutes
    mutable_minimum_age_seconds: u32,

    /// Once a chunk of data within a partition reaches this number of bytes
    /// writes outside its keyspace will be directed to a new chunk
    ///
    /// This chunk will be then compacted once it becomes cold for writes
    /// based on the mutable_linger_seconds and mutable_minimum_age_seconds
    #[structopt(long, default_value = "10485760")] // 10485760 = 10*1024*1024
    mutable_size_threshold: usize,

    /// Once the total amount of buffered data in memory reaches this size start
    /// dropping data from memory based on the drop_order
    #[structopt(long, default_value = "52428800")] // 52428800 = 50*1024*1024
    buffer_size_soft: usize,

    /// Once the amount of data in memory reaches this size start
    /// rejecting writes
    #[structopt(long, default_value = "104857600")] // 104857600 = 100*1024*1024
    buffer_size_hard: usize,

    /// Allow dropping data that has not been persisted to object storage
    /// once the database size has exceeded the configured limits
    #[structopt(long = "drop-persisted-only", parse(from_flag = std::ops::Not::not))]
    drop_non_persisted: bool,

    /// Persists chunks to object storage.
    #[structopt(long = "skip-persist", parse(from_flag = std::ops::Not::not))]
    persist: bool,

    /// Do not allow writing new data to this database
    #[structopt(long)]
    immutable: bool,
}

/// Get list of databases
#[derive(Debug, StructOpt)]
struct List {}

/// Return configuration of specific database
#[derive(Debug, StructOpt)]
struct Get {
    /// The name of the database
    name: String,
}

/// Write data into the specified database
#[derive(Debug, StructOpt)]
struct Write {
    /// The name of the database
    name: String,

    /// File with data to load. Currently supported formats are .lp
    file_name: PathBuf,
}

/// Query the data with SQL
#[derive(Debug, StructOpt)]
struct Query {
    /// The name of the database
    name: String,

    /// The query to run, in SQL format
    query: String,

    /// Optional format ('pretty', 'json', or 'csv')
    #[structopt(short, long, default_value = "pretty")]
    format: String,
}

/// All possible subcommands for database
#[derive(Debug, StructOpt)]
enum Command {
    Create(Create),
    List(List),
    Get(Get),
    Write(Write),
    Query(Query),
    Chunk(chunk::Config),
    Partition(partition::Config),
    Catalog(catalog::Config),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url.clone()).await?;

    match config.command {
        Command::Create(command) => {
            let mut client = management::Client::new(connection);
            let rules = DatabaseRules {
                name: command.name,
                lifecycle_rules: Some(LifecycleRules {
                    mutable_linger_seconds: command.mutable_linger_seconds,
                    mutable_minimum_age_seconds: command.mutable_minimum_age_seconds,
                    mutable_size_threshold: command.mutable_size_threshold as _,
                    buffer_size_soft: command.buffer_size_soft as _,
                    buffer_size_hard: command.buffer_size_hard as _,
                    sort_order: None, // Server-side default
                    drop_non_persisted: command.drop_non_persisted,
                    persist: command.persist,
                    immutable: command.immutable,
                    worker_backoff_millis: Default::default(),
                }),

                // Default to hourly partitions
                partition_template: Some(PartitionTemplate {
                    parts: vec![partition_template::Part {
                        part: Some(partition_template::part::Part::Time(
                            "%Y-%m-%d %H:00:00".into(),
                        )),
                    }],
                }),

                // Note no write buffer config
                ..Default::default()
            };

            client.create_database(rules).await?;

            println!("Ok");
        }
        Command::List(_) => {
            let mut client = management::Client::new(connection);
            let databases = client.list_databases().await?;
            println!("{}", databases.join("\n"))
        }
        Command::Get(get) => {
            let mut client = management::Client::new(connection);
            let database = client.get_database(get.name).await?;
            println!("{}", serde_json::to_string_pretty(&database)?);
        }
        Command::Write(write) => {
            let mut client = write::Client::new(connection);

            let mut file = File::open(&write.file_name).map_err(|e| Error::ReadingFile {
                file_name: write.file_name.clone(),
                source: e,
            })?;

            let mut lp_data = String::new();
            file.read_to_string(&mut lp_data)
                .map_err(|e| Error::ReadingFile {
                    file_name: write.file_name.clone(),
                    source: e,
                })?;

            let lines_written = client.write(write.name, lp_data).await?;

            println!("{} Lines OK", lines_written);
        }
        Command::Query(query) => {
            let mut client = flight::Client::new(connection);
            let Query {
                name,
                format,
                query,
            } = query;

            let format = QueryOutputFormat::from_str(&format)?;

            let mut query_results = client.perform_query(&name, query).await?;

            // It might be nice to do some sort of streaming write
            // rather than buffering the whole thing.
            let mut batches = vec![];
            while let Some(data) = query_results.next().await? {
                batches.push(data);
            }

            let formatted_result = format.format(&batches)?;

            println!("{}", formatted_result);
        }
        Command::Chunk(config) => {
            chunk::command(url, config).await?;
        }
        Command::Partition(config) => {
            partition::command(url, config).await?;
        }
        Command::Catalog(config) => {
            catalog::command(url, config).await?;
        }
    }

    Ok(())
}
