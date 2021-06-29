use std::{collections::BTreeSet, sync::Arc};

use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use parking_lot::Mutex;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use data_types::partition_metadata::{ColumnSummary, InfluxDbType, TableSummary};
use entry::{Sequence, TableBatch};
use internal_types::{
    schema::{builder::SchemaBuilder, InfluxColumnType, Schema},
    selection::Selection,
};
use metrics::GaugeValue;

use crate::column;
use crate::{chunk::snapshot::ChunkSnapshot, column::Column};

pub mod snapshot;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Column error on column {}: {}", column, source))]
    ColumnError {
        column: String,
        source: column::Error,
    },

    #[snafu(display("Column {} had {} rows, expected {}", column, expected, actual))]
    IncorrectRowCount {
        column: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema {
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display("Column not found: {}", column))]
    ColumnNotFound { column: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ChunkMetrics {
    /// keep track of memory used by chunk
    memory_bytes: GaugeValue,
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            memory_bytes: GaugeValue::new_unregistered(),
        }
    }

    pub fn new(_metrics: &metrics::Domain, memory_bytes: GaugeValue) -> Self {
        Self { memory_bytes }
    }
}

/// Represents a Chunk of data (a horizontal subset of a table) in
/// the mutable store.
#[derive(Debug)]
pub struct MBChunk {
    /// The name of this table
    table_name: Arc<str>,

    /// Metrics tracked by this chunk
    metrics: ChunkMetrics,

    /// Map of column id from the chunk dictionary to the column
    columns: HashMap<String, Column>,

    /// Cached chunk snapshot
    ///
    /// Note: This is a mutex to allow mutation within
    /// `Chunk::snapshot()` which only takes an immutable borrow
    snapshot: Mutex<Option<Arc<ChunkSnapshot>>>,
}

impl MBChunk {
    pub fn new(table_name: impl AsRef<str>, metrics: ChunkMetrics) -> Self {
        let table_name = Arc::from(table_name.as_ref());

        let mut chunk = Self {
            table_name,
            columns: Default::default(),
            metrics,
            snapshot: Mutex::new(None),
        };
        chunk.metrics.memory_bytes.set(chunk.size());
        chunk
    }

    /// Write the contents of a [`TableBatch`] into this Chunk.
    ///
    /// Panics if the batch specifies a different name for the table in this Chunk
    pub fn write_table_batch(
        &mut self,
        sequence: Option<&Sequence>,
        batch: TableBatch<'_>,
    ) -> Result<()> {
        let table_name = batch.name();
        assert_eq!(
            table_name,
            self.table_name.as_ref(),
            "can only insert table batch for a single table to chunk"
        );

        let columns = batch.columns();
        self.write_columns(sequence, columns)?;

        // Invalidate chunk snapshot
        *self
            .snapshot
            .try_lock()
            .expect("concurrent readers/writers to MBChunk") = None;

        self.metrics.memory_bytes.set(self.size());

        Ok(())
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(not(feature = "nocache"))]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        let mut guard = self.snapshot.lock();
        if let Some(snapshot) = &*guard {
            return Arc::clone(snapshot);
        }

        let snapshot = Arc::new(ChunkSnapshot::new(
            self,
            self.metrics.memory_bytes.clone_empty(),
        ));
        *guard = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(feature = "nocache")]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        Arc::new(ChunkSnapshot::new(
            self,
            self.metrics.memory_bytes.clone_empty(),
        ))
    }

    /// Return the name of the table in this chunk
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }

    /// Returns the schema for a given selection
    ///
    /// If Selection::All the returned columns are sorted by name
    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {
        let mut schema_builder = SchemaBuilder::new();
        let schema = match selection {
            Selection::All => {
                for (column_name, column) in self.columns.iter() {
                    schema_builder.influx_column(column_name, column.influx_type());
                }

                schema_builder
                    .build()
                    .context(InternalSchema)?
                    .sort_fields_by_name()
            }
            Selection::Some(cols) => {
                for col in cols {
                    let column = self.column(col)?;
                    schema_builder.influx_column(col, column.influx_type());
                }
                schema_builder.build().context(InternalSchema)?
            }
        };

        Ok(schema)
    }

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
    pub fn to_arrow(&self, selection: Selection<'_>) -> Result<RecordBatch> {
        let schema = self.schema(selection)?;
        let columns = schema
            .iter()
            .map(|(_, field)| {
                let column = self
                    .columns
                    .get(field.name())
                    .expect("schema contains non-existent column");

                column.to_arrow().context(ColumnError {
                    column: field.name(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(schema.into(), columns).context(ArrowError {})
    }

    /// Returns a vec of the summary statistics of the tables in this chunk
    pub fn table_summary(&self) -> TableSummary {
        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|(column_name, c)| ColumnSummary {
                name: column_name.to_string(),
                stats: c.stats(),
                influxdb_type: Some(match c.influx_type() {
                    InfluxColumnType::Tag => InfluxDbType::Tag,
                    InfluxColumnType::Field(_) => InfluxDbType::Field,
                    InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
                }),
            })
            .collect();

        columns.sort_by(|a, b| a.name.cmp(&b.name));

        TableSummary {
            name: self.table_name.to_string(),
            columns,
        }
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    ///
    /// Note: This does not include the size of any cached ChunkSnapshot
    pub fn size(&self) -> usize {
        // TODO: Better accounting of non-column data (#1565)
        self.columns
            .iter()
            .map(|(k, v)| k.len() + v.size())
            .sum::<usize>()
            + self.table_name.len()
    }

    /// Returns an iterator over (column_name, estimated_size) for all
    /// columns in this chunk.
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.columns
            .iter()
            .map(|(column_name, c)| (column_name.as_str(), c.size()))
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.columns
            .values()
            .next()
            .map(|col| col.len())
            .unwrap_or(0)
    }

    /// Returns a reference to the specified column
    pub(crate) fn column(&self, column: &str) -> Result<&Column> {
        self.columns.get(column).context(ColumnNotFound { column })
    }

    /// Validates the schema of the passed in columns, then adds their values to
    /// the associated columns in the table and updates summary statistics.
    pub fn write_columns(
        &mut self,
        _sequence: Option<&Sequence>,
        columns: Vec<entry::Column<'_>>,
    ) -> Result<()> {
        let row_count_before_insert = self.rows();
        let additional_rows = columns.first().map(|x| x.row_count).unwrap_or_default();
        let final_row_count = row_count_before_insert + additional_rows;

        // get the column ids and validate schema for those that already exist
        columns.iter().try_for_each(|column| {
            ensure!(
                column.row_count == additional_rows,
                IncorrectRowCount {
                    column: column.name(),
                    expected: additional_rows,
                    actual: column.row_count,
                }
            );

            if let Some(c) = self.columns.get(column.name()) {
                c.validate_schema(&column).context(ColumnError {
                    column: column.name(),
                })?;
            }

            Ok(())
        })?;

        for fb_column in columns {
            let influx_type = fb_column.influx_type();

            let column = self
                .columns
                .raw_entry_mut()
                .from_key(fb_column.name())
                .or_insert_with(|| {
                    (
                        fb_column.name().to_string(),
                        Column::new(row_count_before_insert, influx_type),
                    )
                })
                .1;

            column.append(&fb_column).context(ColumnError {
                column: fb_column.name(),
            })?;

            assert_eq!(column.len(), final_row_count);
        }

        for c in self.columns.values_mut() {
            c.push_nulls_to_len(final_row_count);
        }

        Ok(())
    }
}

pub mod test_helpers {
    use entry::test_helpers::lp_to_entry;

    use super::*;

    /// A helper that will write line protocol string to the passed in Chunk.
    /// All data will be under a single partition with a clock value and
    /// server id of 1.
    pub fn write_lp_to_chunk(lp: &str, chunk: &mut MBChunk) -> Result<()> {
        let entry = lp_to_entry(lp);

        for w in entry.partition_writes().unwrap() {
            let table_batches = w.table_batches();
            // ensure they are all to the same table
            let table_names: BTreeSet<String> =
                table_batches.iter().map(|b| b.name().to_string()).collect();

            assert!(
                table_names.len() <= 1,
                "Can only write 0 or one tables to chunk. Found {:?}",
                table_names
            );

            for batch in table_batches {
                let seq = Some(Sequence::new(1, 5));
                chunk.write_table_batch(seq.as_ref(), batch)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;

    use entry::test_helpers::lp_to_entry;
    use internal_types::schema::{InfluxColumnType, InfluxFieldType};

    use super::*;
    use std::num::NonZeroU64;

    use arrow_util::assert_batches_eq;
    use data_types::partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics};

    use super::test_helpers::write_lp_to_chunk;

    #[test]
    fn writes_table_batches() {
        let mut chunk = MBChunk::new("cpu", ChunkMetrics::new_unregistered());

        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk)
        );
    }

    #[test]
    fn writes_table_3_batches() {
        let mut chunk = MBChunk::new("cpu", ChunkMetrics::new_unregistered());

        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let lp = vec!["cpu,host=c val=11 1"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let lp = vec!["cpu,host=a val=14 2"].join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "| c    | 1970-01-01 00:00:00.000000001 | 11  |",
                "| a    | 1970-01-01 00:00:00.000000002 | 14  |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk)
        );
    }

    #[test]
    fn test_summary() {
        let mut chunk = MBChunk::new("cpu", ChunkMetrics::new_unregistered());
        let lp = r#"
            cpu,host=a val=23 1
            cpu,host=b,env=prod val=2 1
            cpu,host=c,env=stage val=11 1
            cpu,host=a,env=prod val=14 2
        "#;
        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let summary = chunk.table_summary();
        assert_eq!(
            summary,
            TableSummary {
                name: "cpu".to_string(),
                columns: vec![
                    ColumnSummary {
                        name: "env".to_string(),
                        influxdb_type: Some(InfluxDbType::Tag),
                        stats: Statistics::String(StatValues {
                            min: Some("prod".to_string()),
                            max: Some("stage".to_string()),
                            count: 3,
                            distinct_count: Some(NonZeroU64::new(3).unwrap())
                        })
                    },
                    ColumnSummary {
                        name: "host".to_string(),
                        influxdb_type: Some(InfluxDbType::Tag),
                        stats: Statistics::String(StatValues {
                            min: Some("a".to_string()),
                            max: Some("c".to_string()),
                            count: 4,
                            distinct_count: Some(NonZeroU64::new(3).unwrap())
                        })
                    },
                    ColumnSummary {
                        name: "time".to_string(),
                        influxdb_type: Some(InfluxDbType::Timestamp),
                        stats: Statistics::I64(StatValues {
                            min: Some(1),
                            max: Some(2),
                            count: 4,
                            distinct_count: None
                        })
                    },
                    ColumnSummary {
                        name: "val".to_string(),
                        influxdb_type: Some(InfluxDbType::Field),
                        stats: Statistics::F64(StatValues {
                            min: Some(2.),
                            max: Some(23.),
                            count: 4,
                            distinct_count: None
                        })
                    },
                ]
            }
        )
    }

    #[test]
    #[cfg(not(feature = "nocache"))]
    fn test_snapshot() {
        let mut chunk = MBChunk::new("cpu", ChunkMetrics::new_unregistered());

        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");

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

    fn chunk_to_batches(chunk: &MBChunk) -> Vec<RecordBatch> {
        vec![chunk.to_arrow(Selection::All).unwrap()]
    }

    #[test]
    fn table_size() {
        let mut table = MBChunk::new("table_name", ChunkMetrics::new_unregistered());

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, lp_lines.clone());
        let s1 = table.size();

        write_lines_to_table(&mut table, lp_lines.clone());
        let s2 = table.size();

        write_lines_to_table(&mut table, lp_lines);
        let s3 = table.size();

        // Should increase by a constant amount each time
        assert_eq!(s2 - s1, s3 - s2);
    }

    #[test]
    fn test_to_arrow_schema_all() {
        let mut table = MBChunk::new("table_name", ChunkMetrics::new_unregistered());

        let lp_lines = vec![
            "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,uint_field=42u,bool_field=t,string_field=\"foo\" 100",
        ];

        write_lines_to_table(&mut table, lp_lines);

        let selection = Selection::All;
        let actual_schema = table.schema(selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("bool_field", ArrowDataType::Boolean)
            .tag("city")
            .field("float_field", ArrowDataType::Float64)
            .field("int_field", ArrowDataType::Int64)
            .tag("state")
            .field("string_field", ArrowDataType::Utf8)
            .timestamp()
            .field("uint_field", ArrowDataType::UInt64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn test_to_arrow_schema_subset() {
        let mut table = MBChunk::new("table_name", ChunkMetrics::new_unregistered());

        let lp_lines = vec!["h2o,state=MA,city=Boston float_field=70.4 100"];

        write_lines_to_table(&mut table, lp_lines);

        let selection = Selection::Some(&["float_field"]);
        let actual_schema = table.schema(selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("float_field", ArrowDataType::Float64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn write_columns_validates_schema() {
        let mut table = MBChunk::new("table_name", ChunkMetrics::new_unregistered());
        let sequencer_id = 1;
        let sequence_number = 5;
        let sequence = Some(Sequence::new(sequencer_id, sequence_number));

        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let entry = lp_to_entry(&lp);
        table
            .write_columns(
                sequence.as_ref(),
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .unwrap();

        let lp = "foo t1=\"string\" 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                sequence.as_ref(),
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Tag,
                        inserted: InfluxColumnType::Field(InfluxFieldType::String)
                    }
                } if column == "t1"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo iv=1u 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                sequence.as_ref(),
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        inserted: InfluxColumnType::Field(InfluxFieldType::UInteger),
                        existing: InfluxColumnType::Field(InfluxFieldType::Integer)
                    }
                } if column == "iv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo fv=1i 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                sequence.as_ref(),
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Float),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Integer)
                    }
                } if column == "fv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo bv=1 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                sequence.as_ref(),
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Boolean),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Float)
                    }
                } if column == "bv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo sv=true 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                sequence.as_ref(),
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::String),
                        inserted: InfluxColumnType::Field(InfluxFieldType::Boolean),
                    }
                } if column == "sv"
            ),
            "didn't match returned error: {:?}",
            response
        );

        let lp = "foo,sv=\"bar\" f=3i 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                sequence.as_ref(),
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::ColumnError {
                    column,
                    source: column::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::String),
                        inserted: InfluxColumnType::Tag,
                    }
                } if column == "sv"
            ),
            "didn't match returned error: {:?}",
            response
        );
    }

    ///  Insert the line protocol lines in `lp_lines` into this table
    fn write_lines_to_table(table: &mut MBChunk, lp_lines: Vec<&str>) {
        let lp_data = lp_lines.join("\n");
        let entry = lp_to_entry(&lp_data);

        let sequence = Some(Sequence::new(1, 5));
        for batch in entry
            .partition_writes()
            .unwrap()
            .first()
            .unwrap()
            .table_batches()
        {
            table
                .write_columns(sequence.as_ref(), batch.columns())
                .unwrap();
        }
    }
}
