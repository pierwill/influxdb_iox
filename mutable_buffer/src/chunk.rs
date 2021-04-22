//! Represents a Chunk of data (a collection of tables and their data within
//! some chunk) in the mutable store.
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};

use arrow_deps::arrow::record_batch::RecordBatch;
use data_types::{database_rules::WriterId, partition_metadata::TableSummary, ClockValue};
use internal_types::{
    entry::TableBatch,
    selection::Selection,
};
use tracker::{MemRegistry, MemTracker};

use crate::chunk::snapshot::ChunkSnapshot;
use crate::{
    dictionary::{Dictionary, Error as DictionaryError, DID},
    table::Table,
};
use parking_lot::Mutex;

pub mod snapshot;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing table '{}': {}", table_name, source))]
    TableWrite {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display("Table Error in '{}': {}", table_name, source))]
    NamedTableError {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display("Table {} not found in chunk {}", table, chunk))]
    TableNotFoundInChunk { table: DID, chunk: u64 },

    #[snafu(display("Column ID {} not found in dictionary of chunk {}", column_id, chunk))]
    ColumnIdNotFoundInDictionary {
        column_id: DID,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column name {} not found in dictionary of chunk {}",
        column_name,
        chunk_id
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        chunk_id: u64,
        source: DictionaryError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Chunk {
    /// The id for this chunk
    id: u32,

    /// `dictionary` maps &str -> DID. The DIDs are used in place of String or
    /// str to avoid slow string operations. The same dictionary is used for
    /// table names, tag names, tag values, and column names.
    // TODO: intern string field values too?
    dictionary: Dictionary,

    /// map of the dictionary ID for the table name to the table
    tables: HashMap<DID, Table>,

    /// keep track of memory used by chunk
    tracker: MemTracker,

    /// Cached chunk snapshot
    ///
    /// Note: This is a mutex to allow mutation within
    /// `Chunk::snapshot()` which only takes an immutable borrow
    snapshot: Mutex<Option<Arc<ChunkSnapshot>>>,
}

impl Chunk {
    pub fn new(id: u32, memory_registry: &MemRegistry) -> Self {
        let mut chunk = Self {
            id,
            dictionary: Dictionary::new(),
            tables: HashMap::new(),
            tracker: memory_registry.register(),
            snapshot: Mutex::new(None),
        };
        chunk.tracker.set_bytes(chunk.size());
        chunk
    }

    pub fn write_table_batches(
        &mut self,
        clock_value: ClockValue,
        writer_id: WriterId,
        batches: &[TableBatch<'_>],
    ) -> Result<()> {
        for batch in batches {
            let table_name = batch.name();
            let table_id = self.dictionary.lookup_value_or_insert(table_name);

            let table = self
                .tables
                .entry(table_id)
                .or_insert_with(|| Table::new(table_id));

            let columns = batch.columns();
            table
                .write_columns(&mut self.dictionary, clock_value, writer_id, columns)
                .context(TableWrite { table_name })?;
        }

        // Invalidate chunk snapshot
        *self
            .snapshot
            .try_lock()
            .expect("concurrent readers/writers to MBChunk") = None;

        self.tracker.set_bytes(self.size());

        Ok(())
    }

    // Add all tables names in this chunk to `names` if they are not already present
    pub fn all_table_names(&self, names: &mut BTreeSet<String>) {
        for &table_id in self.tables.keys() {
            let table_name = self.dictionary.lookup_id(table_id).unwrap();
            if !names.contains(table_name) {
                names.insert(table_name.to_string());
            }
        }
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(not(feature = "nocache"))]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        let mut guard = self.snapshot.lock();
        if let Some(snapshot) = &*guard {
            return Arc::clone(snapshot);
        }

        // TODO: Incremental snapshot generation
        let snapshot = Arc::new(ChunkSnapshot::new(self));
        *guard = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(feature = "nocache")]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        Arc::new(ChunkSnapshot::new(self))
    }

    /// returns true if there is no data in this chunk
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// return the ID of this chunk
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
    pub fn table_to_arrow(
        &self,
        dst: &mut Vec<RecordBatch>,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<()> {
        if let Some(table) = self.table(table_name)? {
            dst.push(
                table
                    .to_arrow(&self.dictionary, selection)
                    .context(NamedTableError { table_name })?,
            );
        }
        Ok(())
    }

    /// Returns a vec of the summary statistics of the tables in this chunk
    pub fn table_summaries(&self) -> Vec<TableSummary> {
        self.tables
            .iter()
            .map(|(&table_id, table)| {
                let name = self
                    .dictionary
                    .lookup_id(table_id)
                    .expect("table name not found in dictionary");

                TableSummary {
                    name: name.to_string(),
                    columns: table.stats(&self.dictionary),
                }
            })
            .collect()
    }

    /// Returns the named table, or None if no such table exists in this chunk
    fn table(&self, table_name: &str) -> Result<Option<&Table>> {
        let table_id = self.dictionary.lookup_value(table_name);

        let table = match table_id {
            Ok(table_id) => Some(self.tables.get(&table_id).context(TableNotFoundInChunk {
                table: table_id,
                chunk: self.id,
            })?),
            Err(_) => None,
        };
        Ok(table)
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        let data_size = self.tables.values().fold(0, |acc, val| acc + val.size());
        data_size + self.dictionary.size
    }

    /// Return true if this chunk has the specified table name
    pub fn has_table(&self, table_name: &str) -> bool {
        matches!(self.table(table_name), Ok(Some(_)))
    }
}

pub mod test_helpers {
    use super::*;
    use internal_types::entry::test_helpers::lp_to_entry;

    /// A helper that will write line protocol string to the passed in Chunk.
    /// All data will be under a single partition with a clock value and
    /// writer id of 0.
    pub fn write_lp_to_chunk(lp: &str, chunk: &mut Chunk) -> Result<()> {
        let entry = lp_to_entry(lp);

        for w in entry.partition_writes().unwrap() {
            chunk.write_table_batches(ClockValue::new(0), 0, &w.table_batches())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::write_lp_to_chunk;
    use super::*;
    use arrow_deps::assert_table_eq;

    #[test]
    fn writes_table_batches() {
        let mr = MemRegistry::new();
        let mut chunk = Chunk::new(1, &mr);

        let lp = vec![
            "cpu,host=a val=23 1",
            "cpu,host=b val=2 1",
            "mem,host=a val=23432i 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_table_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk, "cpu")
        );

        assert_table_eq!(
            vec![
                "+------+-------------------------------+-------+",
                "| host | time                          | val   |",
                "+------+-------------------------------+-------+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23432 |",
                "+------+-------------------------------+-------+",
            ],
            &chunk_to_batches(&chunk, "mem")
        );
    }

    #[test]
    fn writes_table_3_batches() {
        let mr = MemRegistry::new();
        let mut chunk = Chunk::new(1, &mr);

        let lp = vec![
            "cpu,host=a val=23 1",
            "cpu,host=b val=2 1",
            "mem,host=a val=23432i 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let lp = vec![
            "cpu,host=c val=11 1",
            "mem sval=\"hi\" 2",
            "disk val=true 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_table_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "| c    | 1970-01-01 00:00:00.000000001 | 11  |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk, "cpu")
        );

        assert_table_eq!(
            vec![
                "+-------------------------------+------+",
                "| time                          | val  |",
                "+-------------------------------+------+",
                "| 1970-01-01 00:00:00.000000001 | true |",
                "+-------------------------------+------+",
            ],
            &chunk_to_batches(&chunk, "disk")
        );

        assert_table_eq!(
            vec![
                "+------+------+-------------------------------+-------+",
                "| host | sval | time                          | val   |",
                "+------+------+-------------------------------+-------+",
                "| a    |      | 1970-01-01 00:00:00.000000001 | 23432 |",
                "|      | hi   | 1970-01-01 00:00:00.000000002 |       |",
                "+------+------+-------------------------------+-------+",
            ],
            &chunk_to_batches(&chunk, "mem")
        );
    }

    #[test]
    fn test_snapshot() {
        let mr = MemRegistry::new();
        let mut chunk = Chunk::new(1, &mr);

        let lp = vec![
            "cpu,host=a val=23 1",
            "cpu,host=b val=2 1",
            "mem,host=a val=23432i 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s1 = chunk.snapshot();
        let s2 = chunk.snapshot();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s3 = chunk.snapshot();
        let s4 = chunk.snapshot();

        assert_eq!(Arc::as_ptr(&s1), Arc::as_ptr(&s2));
        assert_ne!(Arc::as_ptr(&s1), Arc::as_ptr(&s3));
        assert_eq!(Arc::as_ptr(&s3), Arc::as_ptr(&s4));
    }

    fn chunk_to_batches(chunk: &Chunk, table: &str) -> Vec<RecordBatch> {
        let mut batches = vec![];
        chunk
            .table_to_arrow(&mut batches, table, Selection::All)
            .unwrap();
        batches
    }
}
