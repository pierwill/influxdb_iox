use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryFrom,
};

use metrics::{Gauge, GaugeValue, KeyValue};
use snafu::{ResultExt, Snafu};

use arrow::record_batch::RecordBatch;
use data_types::{chunk_metadata::ChunkColumnSummary, partition_metadata::TableSummary};
use internal_types::{schema::builder::Error as SchemaError, schema::Schema, selection::Selection};
use observability_deps::tracing::info;

use crate::row_group::{ColumnName, Predicate};
use crate::schema::{AggregateType, ResultSchema};
use crate::table;
use crate::table::Table;
use crate::{column::Statistics, row_group::RowGroup};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unsupported operation: {}", msg))]
    UnsupportedOperation { msg: String },

    #[snafu(display("error processing table: {}", source))]
    TableError { source: table::Error },

    #[snafu(display("error generating schema for table: {}", source))]
    TableSchemaError { source: SchemaError },

    #[snafu(display("table '{}' does not exist", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("column '{}' does not exist in table '{}'", column_name, table_name))]
    ColumnDoesNotExist {
        column_name: String,
        table_name: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `Chunk` is a horizontal partition of data for a single table.
pub struct Chunk {
    // All metrics for the chunk.
    metrics: ChunkMetrics,

    // The table associated with the chunk.
    pub(crate) table: Table,
}

impl Chunk {
    /// Initialises a new `Chunk` with the associated chunk ID.
    pub fn new(table_name: impl Into<String>, metrics: ChunkMetrics) -> Self {
        Self {
            metrics,
            table: Table::new(table_name.into()),
        }
    }

    // The total size taken up by an empty instance of `Chunk`.
    fn base_size() -> usize {
        std::mem::size_of::<Self>()
    }

    /// The total estimated size in bytes of this `Chunk` and all contained
    /// data.
    pub fn size(&self) -> usize {
        Self::base_size() + self.table.size()
    }

    /// Return the estimated size for each column in the table.
    /// Note there may be multiple entries for each column.
    pub fn column_sizes(&self) -> Vec<ChunkColumnSummary> {
        self.table.column_sizes()
    }

    /// The total estimated size in bytes of this `Chunk` and all contained
    /// data if the data was not compressed but was stored contiguously in
    /// vectors. `include_nulls` allows the caller to factor in NULL values or
    /// to ignore them.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        self.table.size_raw(include_nulls)
    }

    /// The total number of rows in all row groups in all tables in this chunk.
    pub fn rows(&self) -> u64 {
        self.table.rows()
    }

    /// The total number of row groups in all tables in this chunk.
    pub(crate) fn row_groups(&self) -> usize {
        self.table.row_groups()
    }

    /// Add a row_group to a table in the chunk, updating all Chunk meta data.
    pub(crate) fn upsert_table_with_row_group(&mut self, row_group: RowGroup) {
        // track new row group statistics to update column-based metrics.
        let storage_statistics = row_group.column_storage_statistics();

        self.table.add_row_group(row_group);

        // Get and set new size of chunk on memory tracker
        let size = Self::base_size() + self.table.size();
        self.metrics.memory_bytes.set(size);

        // update column metrics associated with column storage
        self.metrics
            .update_column_storage_statistics(&storage_statistics);
    }

    /// Add a record batch of data to to a `Table` in the chunk.
    ///
    /// The data is converted to a `RowGroup` outside of any locking so the
    /// caller does not need to be concerned about the size of the update.
    pub fn upsert_table(&mut self, table_name: &str, table_data: RecordBatch) {
        // TEMPORARY: print record batch information
        for (column, field) in table_data
            .columns()
            .iter()
            .zip(table_data.schema().fields())
        {
            info!(%table_name, column = %field.name(), rows=column.len(), data_type=%field.data_type(), buffer_size=column.get_buffer_memory_size(), array_size=column.get_array_memory_size(), "column");
            for (idx, buffer) in column.data().buffers().iter().enumerate() {
                info!(%table_name, column = %field.name(), len=buffer.len(), capacity=buffer.capacity(), idx, "column data");
            }

            for (parent_idx, data) in column.data().child_data().iter().enumerate() {
                for (child_idx, buffer) in data.buffers().iter().enumerate() {
                    info!(%table_name, column = %field.name(), len=buffer.len(), capacity=buffer.capacity(), parent_idx, child_idx, "column child data");
                }
            }
        }

        // Approximate heap size of record batch.
        let mub_rb_size = table_data
            .columns()
            .iter()
            .map(|c| c.get_buffer_memory_size())
            .sum::<usize>();
        let columns = table_data.num_columns();

        // This call is expensive. Complete it before locking.
        let now = std::time::Instant::now();
        let row_group = RowGroup::from(table_data);
        let compressing_took = now.elapsed();

        let rows = row_group.rows();
        let rg_size = row_group.size();
        let mub_rb_comp = format!(
            "{:.2}%",
            (1.0 - (rg_size as f64 / mub_rb_size as f64)) * 100.0
        );

        let raw_size_null = row_group.size_raw(true);
        let raw_size_no_null = row_group.size_raw(false);
        let raw_rb_comp = format!(
            "{:.2}%",
            (1.0 - (rg_size as f64 / raw_size_null as f64)) * 100.0
        );
        let table_name = self.table.name();

        info!(%rows, %columns, rg_size, mub_rb_size, %mub_rb_comp, raw_size_null, raw_size_no_null, %raw_rb_comp, ?table_name, ?compressing_took, "row group added");

        self.upsert_table_with_row_group(row_group)
    }

    //
    // Methods for executing queries.
    //

    /// Returns selected data for the specified columns.
    ///
    /// Results may be filtered by conjunctive predicates.
    /// The `ReadBuffer` will optimally prune columns and row groups to improve
    /// execution where possible.
    ///
    /// `read_filter` return an iterator that will emit record batches for all
    /// row groups help under the provided chunks.
    ///
    /// `read_filter` is lazy - it does not execute against the next row group
    /// until the results for the previous one have been emitted.
    pub fn read_filter(
        &self,
        _table_name: &str,
        predicate: Predicate,
        select_columns: Selection<'_>,
    ) -> table::ReadFilterResults {
        self.table.read_filter(&select_columns, &predicate)
    }

    /// Returns an iterable collection of data in group columns and aggregate
    /// columns, optionally filtered by the provided predicate. Results are
    /// merged across all row groups.
    ///
    /// Note: `read_aggregate` currently only supports grouping on "tag"
    /// columns.
    pub(crate) fn read_aggregate(
        &self,
        predicate: Predicate,
        group_columns: &Selection<'_>,
        aggregates: &[(ColumnName<'_>, AggregateType)],
    ) -> Result<table::ReadAggregateResults> {
        self.table
            .read_aggregate(predicate, group_columns, aggregates)
            .context(TableError)
    }

    //
    // ---- Schema queries
    //

    /// Determines if one of more rows in the provided table could possibly
    /// match the provided predicate.
    ///
    /// If the provided table does not exist then `could_pass_predicate` returns
    /// `false`.
    pub fn could_pass_predicate(&self, predicate: Predicate) -> bool {
        self.table.could_pass_predicate(&predicate)
    }

    /// Return table summaries or all tables in this chunk.
    /// Each table will be represented exactly once.
    ///
    /// TODO(edd): consider deprecating or changing to return information about
    /// the physical layout of the data in the chunk.
    pub fn table_summaries(&self) -> Vec<TableSummary> {
        vec![self.table.table_summary()]
    }

    /// Returns a schema object for a `read_filter` operation using the provided
    /// column selection. An error is returned if the specified columns do not
    /// exist.
    ///
    /// TODO: https://github.com/influxdata/influxdb_iox/issues/1717
    pub fn read_filter_table_schema(
        &self,
        _table_name: &str,
        columns: Selection<'_>,
    ) -> Result<Schema> {
        // Validate columns exist in table.
        let table_meta = self.table.meta();
        if let Selection::Some(cols) = columns {
            for column_name in cols {
                if !table_meta.has_column(column_name) {
                    return ColumnDoesNotExist {
                        column_name: column_name.to_string(),
                        table_name: self.table.name().to_string(),
                    }
                    .fail();
                }
            }
        }

        // Build a table schema
        Schema::try_from(&ResultSchema {
            select_columns: match columns {
                Selection::All => table_meta.schema_for_all_columns(),
                Selection::Some(column_names) => table_meta.schema_for_column_names(column_names),
            },
            ..ResultSchema::default()
        })
        .context(TableSchemaError)
    }

    /// Determines if at least one row in the Chunk satisfies the provided
    /// predicate. `satisfies_predicate` will return true if it is guaranteed
    /// that at least one row in the Chunk will satisfy the predicate.
    pub fn satisfies_predicate(&self, predicate: &Predicate) -> bool {
        self.table.satisfies_predicate(predicate)
    }

    /// Returns the distinct set of column names that contain data matching the
    /// provided predicate, which may be empty.
    ///
    /// Results can be further limited to a specific selection of columns.
    ///
    /// `dst` is a buffer that will be populated with results. `column_names` is
    /// smart enough to short-circuit processing on row groups when it
    /// determines that all the columns in the row group are already contained
    /// in the results buffer. Callers can skip this behaviour by passing in
    /// an empty `BTreeSet`.
    ///
    /// TODO(edd): remove `table_name`
    pub fn column_names(
        &self,
        _table_name: &str,
        predicate: Predicate,
        only_columns: Selection<'_>,
        dst: BTreeSet<String>,
    ) -> Result<BTreeSet<String>> {
        Ok(self.table.column_names(&predicate, only_columns, dst))
    }

    /// Returns the distinct set of column values for each provided column,
    /// where each returned value lives in a row matching the provided
    /// predicate. All values are deduplicated across row groups in the table.
    ///
    /// If the predicate is empty then all distinct values are returned for the
    /// table.
    ///
    /// Returns an error if the provided table does not exist.
    ///
    /// `dst` is intended to allow for some more sophisticated execution,
    /// wherein execution can be short-circuited for distinct values that have
    /// already been found. Callers can simply provide an empty `BTreeMap` to
    /// skip this behaviour.
    pub fn column_values(
        &self,
        _table_name: &str,
        predicate: Predicate,
        columns: Selection<'_>,
        dst: BTreeMap<String, BTreeSet<String>>,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let columns = match columns {
            Selection::All => {
                return UnsupportedOperation {
                    msg: "column_values does not support All columns".to_owned(),
                }
                .fail();
            }
            Selection::Some(columns) => columns,
        };

        self.table
            .column_values(&predicate, columns, dst)
            .context(TableError)
    }
}

impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Chunk: rows: {:?}", self.rows())
    }
}

#[derive(Debug)]
pub struct ChunkMetrics {
    /// keep track of memory used by table data in chunk
    memory_bytes: GaugeValue,

    // This metric tracks the total number of columns in read buffer.
    columns_total: Gauge,

    // This metric tracks the total number of values stored in read buffer
    // column encodings further segmented by nullness.
    column_values_total: Gauge,

    // This metric tracks the total number of bytes used by read buffer columns
    column_bytes_total: Gauge,

    // This metric tracks an estimated uncompressed data size for read buffer
    // columns, further segmented by nullness. It is a building block for
    // tracking a measure of overall compression.
    column_raw_bytes_total: metrics::Gauge,
}

impl ChunkMetrics {
    pub fn new(domain: &metrics::Domain, memory_bytes: GaugeValue) -> Self {
        Self {
            memory_bytes,
            columns_total: domain.register_gauge_metric(
                "column",
                Some("total"),
                "The number of columns within the Read Buffer",
            ),
            column_values_total: domain.register_gauge_metric(
                "column",
                Some("values"),
                "The number of values within columns in the Read Buffer",
            ),
            column_bytes_total: domain.register_gauge_metric(
                "column",
                Some("bytes"),
                "The number of bytes used by all columns in the Read Buffer",
            ),
            column_raw_bytes_total: domain.register_gauge_metric(
                "column_raw",
                Some("bytes"),
                "The number of bytes used by all columns if they were uncompressed in the Read Buffer",
            ),
        }
    }

    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            memory_bytes: GaugeValue::new_unregistered(),
            columns_total: Gauge::new_unregistered(),
            column_values_total: Gauge::new_unregistered(),
            column_bytes_total: Gauge::new_unregistered(),
            column_raw_bytes_total: Gauge::new_unregistered(),
        }
    }

    // Updates column storage statistics for the Read Buffer.
    fn update_column_storage_statistics(&mut self, statistics: &[Statistics]) {
        for stat in statistics {
            let labels = &[
                KeyValue::new("encoding", stat.enc_type.clone()),
                KeyValue::new("log_data_type", stat.log_data_type),
            ];

            // update number of columns
            self.columns_total.inc(1, labels);

            // update bytes associated with columns
            self.column_bytes_total.inc(stat.bytes, labels);

            // update raw estimated bytes of NULL values
            self.column_raw_bytes_total.inc(
                stat.raw_bytes - stat.raw_bytes_no_null,
                &[
                    KeyValue::new("encoding", stat.enc_type.clone()),
                    KeyValue::new("log_data_type", stat.log_data_type),
                    KeyValue::new("null", "true"),
                ],
            );

            // update raw estimated bytes of non-NULL values
            self.column_raw_bytes_total.inc(
                stat.raw_bytes_no_null,
                &[
                    KeyValue::new("encoding", stat.enc_type.clone()),
                    KeyValue::new("log_data_type", stat.log_data_type),
                    KeyValue::new("null", "false"),
                ],
            );

            // update number of NULL values
            self.column_values_total.inc(
                stat.nulls as usize,
                &[
                    KeyValue::new("encoding", stat.enc_type.clone()),
                    KeyValue::new("log_data_type", stat.log_data_type),
                    KeyValue::new("null", "true"),
                ],
            );

            // update number of non-NULL values
            self.column_values_total.inc(
                (stat.values - stat.nulls) as usize,
                &[
                    KeyValue::new("encoding", stat.enc_type.clone()),
                    KeyValue::new("log_data_type", stat.log_data_type),
                    KeyValue::new("null", "false"),
                ],
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
            TimestampNanosecondArray, UInt64Array,
        },
        datatypes::DataType::{Boolean, Float64, Int64, UInt64, Utf8},
    };
    use data_types::partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics};
    use internal_types::schema::builder::SchemaBuilder;

    use super::*;
    use crate::BinaryExpr;
    use crate::{
        row_group::{ColumnType, RowGroup},
        value::Values,
    };
    use arrow::array::DictionaryArray;
    use arrow::datatypes::Int32Type;
    use std::num::NonZeroU64;

    // helper to make the `add_remove_tables` test simpler to read.
    fn gen_recordbatch() -> RecordBatch {
        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .non_null_field("active", Boolean)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["west", "west", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        RecordBatch::try_new(schema, data).unwrap()
    }

    // Helper function to assert the contents of a column on a record batch.
    fn assert_rb_column_equals(rb: &RecordBatch, col_name: &str, exp: &Values<'_>) {
        use arrow::datatypes::DataType;

        let got_column = rb.column(rb.schema().index_of(col_name).unwrap());

        match exp {
            Values::Dictionary(keys, values) => match got_column.data_type() {
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    // Record batch stores keys as i32
                    let keys = keys
                        .iter()
                        .map(|&x| i32::try_from(x).unwrap())
                        .collect::<Vec<_>>();

                    let dictionary = got_column
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .unwrap();
                    let rb_values = dictionary.values();
                    let rb_values = rb_values.as_any().downcast_ref::<StringArray>().unwrap();

                    // Ensure string values are same
                    assert!(rb_values.iter().zip(values.iter()).all(|(a, b)| &a == b));

                    let rb_keys = dictionary.keys().values();
                    assert_eq!(rb_keys, keys.as_slice());
                }
                d => panic!("Unexpected type {:?}", d),
            },
            Values::String(exp_data) => match got_column.data_type() {
                DataType::Utf8 => {
                    let arr = got_column.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(&arr.iter().collect::<Vec<_>>(), exp_data);
                }
                d => panic!("Unexpected type {:?}", d),
            },
            Values::I64(exp_data) => {
                if let Some(arr) = got_column.as_any().downcast_ref::<Int64Array>() {
                    assert_eq!(arr.values(), exp_data);
                } else if let Some(arr) = got_column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                {
                    assert_eq!(arr.values(), exp_data);
                } else {
                    panic!("Unexpected type");
                }
            }
            Values::U64(exp_data) => {
                let arr: &UInt64Array = got_column.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            Values::F64(exp_data) => {
                let arr: &Float64Array =
                    got_column.as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(arr.values(), exp_data);
            }
            Values::I64N(exp_data) => {
                let arr: &Int64Array = got_column.as_any().downcast_ref::<Int64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::U64N(exp_data) => {
                let arr: &UInt64Array = got_column.as_any().downcast_ref::<UInt64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::F64N(exp_data) => {
                let arr: &Float64Array =
                    got_column.as_any().downcast_ref::<Float64Array>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::Bool(exp_data) => {
                let arr: &BooleanArray =
                    got_column.as_any().downcast_ref::<BooleanArray>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
            Values::ByteArray(exp_data) => {
                let arr: &BinaryArray = got_column.as_any().downcast_ref::<BinaryArray>().unwrap();
                let got_data = (0..got_column.len())
                    .map(|i| {
                        if got_column.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&got_data, exp_data);
            }
        }
    }

    #[test]
    fn add_remove_tables() {
        let reg = metrics::TestMetricRegistry::new(Arc::new(metrics::MetricRegistry::new()));
        let registry = reg.registry();
        let domain =
            registry.register_domain_with_labels("read_buffer", vec![KeyValue::new("db", "mydb")]);

        let mut chunk = Chunk::new(
            "a_table",
            ChunkMetrics::new(&domain, GaugeValue::new_unregistered()),
        );

        // Add a new table to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());

        assert_eq!(chunk.rows(), 3);
        assert_eq!(chunk.row_groups(), 1);
        assert!(chunk.size() > 0);

        // Add a row group to the same table in the Chunk.
        let last_chunk_size = chunk.size();
        chunk.upsert_table("a_table", gen_recordbatch());

        assert_eq!(chunk.rows(), 6);
        assert_eq!(chunk.row_groups(), 2);
        assert!(chunk.size() > last_chunk_size);

        assert_eq!(
            String::from_utf8(reg.registry().metrics_as_text()).unwrap(),
            vec![
                "# HELP read_buffer_column_bytes The number of bytes used by all columns in the Read Buffer",
        "# TYPE read_buffer_column_bytes gauge",
        r#"read_buffer_column_bytes{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64"} 72"#,
        r#"read_buffer_column_bytes{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64"} 688"#,
        r#"read_buffer_column_bytes{db="mydb",encoding="FIXED",log_data_type="f64"} 96"#,
        r#"read_buffer_column_bytes{db="mydb",encoding="FIXEDN",log_data_type="bool"} 768"#,
        r#"read_buffer_column_bytes{db="mydb",encoding="RLE",log_data_type="string"} 500"#,
        "# HELP read_buffer_column_raw_bytes The number of bytes used by all columns if they were uncompressed in the Read Buffer",
        "# TYPE read_buffer_column_raw_bytes gauge",
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="false"} 96"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="true"} 0"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="false"} 80"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="true"} 16"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXED",log_data_type="f64",null="false"} 96"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXED",log_data_type="f64",null="true"} 0"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXEDN",log_data_type="bool",null="false"} 54"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXEDN",log_data_type="bool",null="true"} 0"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="RLE",log_data_type="string",null="false"} 216"#,
        r#"read_buffer_column_raw_bytes{db="mydb",encoding="RLE",log_data_type="string",null="true"} 0"#,
        "# HELP read_buffer_column_total The number of columns within the Read Buffer",
        "# TYPE read_buffer_column_total gauge",
        r#"read_buffer_column_total{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64"} 2"#,
        r#"read_buffer_column_total{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64"} 2"#,
        r#"read_buffer_column_total{db="mydb",encoding="FIXED",log_data_type="f64"} 2"#,
        r#"read_buffer_column_total{db="mydb",encoding="FIXEDN",log_data_type="bool"} 2"#,
        r#"read_buffer_column_total{db="mydb",encoding="RLE",log_data_type="string"} 2"#,
        "# HELP read_buffer_column_values The number of values within columns in the Read Buffer",
        "# TYPE read_buffer_column_values gauge",
        r#"read_buffer_column_values{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="false"} 6"#,
        r#"read_buffer_column_values{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="true"} 0"#,
        r#"read_buffer_column_values{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="false"} 4"#,
        r#"read_buffer_column_values{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="true"} 2"#,
        r#"read_buffer_column_values{db="mydb",encoding="FIXED",log_data_type="f64",null="false"} 6"#,
        r#"read_buffer_column_values{db="mydb",encoding="FIXED",log_data_type="f64",null="true"} 0"#,
        r#"read_buffer_column_values{db="mydb",encoding="FIXEDN",log_data_type="bool",null="false"} 6"#,
        r#"read_buffer_column_values{db="mydb",encoding="FIXEDN",log_data_type="bool",null="true"} 0"#,
        r#"read_buffer_column_values{db="mydb",encoding="RLE",log_data_type="string",null="false"} 6"#,
        r#"read_buffer_column_values{db="mydb",encoding="RLE",log_data_type="string",null="true"} 0"#,
        "",
            ]
            .join("\n")
        );

        // when the chunk is dropped the metrics are all correctly decreased
        std::mem::drop(chunk);
        assert_eq!(
            String::from_utf8(reg.registry().metrics_as_text()).unwrap(),
            vec![
                "# HELP read_buffer_column_bytes The number of bytes used by all columns in the Read Buffer",
                "# TYPE read_buffer_column_bytes gauge",
                r#"read_buffer_column_bytes{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64"} 0"#,
                r#"read_buffer_column_bytes{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64"} 0"#,
                r#"read_buffer_column_bytes{db="mydb",encoding="FIXED",log_data_type="f64"} 0"#,
                r#"read_buffer_column_bytes{db="mydb",encoding="FIXEDN",log_data_type="bool"} 0"#,
                r#"read_buffer_column_bytes{db="mydb",encoding="RLE",log_data_type="string"} 0"#,
                "# HELP read_buffer_column_raw_bytes The number of bytes used by all columns if they were uncompressed in the Read Buffer",
                "# TYPE read_buffer_column_raw_bytes gauge",
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="false"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="true"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="false"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="true"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXED",log_data_type="f64",null="false"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXED",log_data_type="f64",null="true"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXEDN",log_data_type="bool",null="false"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="FIXEDN",log_data_type="bool",null="true"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="RLE",log_data_type="string",null="false"} 0"#,
                r#"read_buffer_column_raw_bytes{db="mydb",encoding="RLE",log_data_type="string",null="true"} 0"#,
                "# HELP read_buffer_column_total The number of columns within the Read Buffer",
                "# TYPE read_buffer_column_total gauge",
                r#"read_buffer_column_total{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64"} 0"#,
                r#"read_buffer_column_total{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64"} 0"#,
                r#"read_buffer_column_total{db="mydb",encoding="FIXED",log_data_type="f64"} 0"#,
                r#"read_buffer_column_total{db="mydb",encoding="FIXEDN",log_data_type="bool"} 0"#,
                r#"read_buffer_column_total{db="mydb",encoding="RLE",log_data_type="string"} 0"#,
                "# HELP read_buffer_column_values The number of values within columns in the Read Buffer",
                "# TYPE read_buffer_column_values gauge",
                r#"read_buffer_column_values{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="false"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="BT_U32-FIXED",log_data_type="i64",null="true"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="false"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="FBT_U8-FIXEDN",log_data_type="f64",null="true"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="FIXED",log_data_type="f64",null="false"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="FIXED",log_data_type="f64",null="true"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="FIXEDN",log_data_type="bool",null="false"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="FIXEDN",log_data_type="bool",null="true"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="RLE",log_data_type="string",null="false"} 0"#,
                r#"read_buffer_column_values{db="mydb",encoding="RLE",log_data_type="string",null="true"} 0"#,
                "",
            ]
            .join("\n")
        );
    }

    #[test]
    fn read_filter_table_schema() {
        let mut chunk = Chunk::new("a_table", ChunkMetrics::new_unregistered());

        // Add a new table to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());
        let schema = chunk
            .read_filter_table_schema("a_table", Selection::All)
            .unwrap();

        let exp_schema: Arc<Schema> = SchemaBuilder::new()
            .tag("region")
            .field("counter", Float64)
            .field("active", Boolean)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();
        assert_eq!(Arc::new(schema), exp_schema);

        let schema = chunk
            .read_filter_table_schema(
                "a_table",
                Selection::Some(&["sketchy_sensor", "counter", "region"]),
            )
            .unwrap();

        let exp_schema: Arc<Schema> = SchemaBuilder::new()
            .field("sketchy_sensor", Float64)
            .field("counter", Float64)
            .tag("region")
            .build()
            .unwrap()
            .into();
        assert_eq!(Arc::new(schema), exp_schema);

        // Verify error handling
        assert!(matches!(
            chunk.read_filter_table_schema("a_table", Selection::Some(&["random column name"])),
            Err(Error::ColumnDoesNotExist { .. })
        ));
    }

    #[test]
    fn table_summaries() {
        let mut chunk = Chunk::new("a_table", ChunkMetrics::new_unregistered());

        let schema = SchemaBuilder::new()
            .non_null_tag("env")
            .non_null_field("temp", Float64)
            .non_null_field("counter", UInt64)
            .non_null_field("icounter", Int64)
            .non_null_field("active", Boolean)
            .non_null_field("msg", Utf8)
            .timestamp()
            .build()
            .unwrap();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["prod", "dev", "prod"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![10.0, 30000.0, 4500.0])),
            Arc::new(UInt64Array::from(vec![1000, 3000, 5000])),
            Arc::new(Int64Array::from(vec![1000, -1000, 4000])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
            Arc::new(StringArray::from(vec![Some("msg a"), Some("msg b"), None])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
        ];

        // Add a record batch to a single partition
        let rb = RecordBatch::try_new(schema.into(), data).unwrap();
        // The row group gets added to the same chunk each time.
        chunk.upsert_table("a_table", rb);

        let summaries = chunk.table_summaries();
        let expected = vec![TableSummary {
            name: "a_table".into(),
            columns: vec![
                ColumnSummary {
                    name: "active".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::Bool(StatValues::new(Some(false), Some(true), 3)),
                },
                ColumnSummary {
                    name: "counter".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::U64(StatValues::new(Some(1000), Some(5000), 3)),
                },
                ColumnSummary {
                    name: "env".into(),
                    influxdb_type: Some(InfluxDbType::Tag),
                    stats: Statistics::String(StatValues {
                        min: Some("dev".into()),
                        max: Some("prod".into()),
                        count: 3,
                        distinct_count: Some(NonZeroU64::new(2).unwrap()),
                    }),
                },
                ColumnSummary {
                    name: "icounter".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::I64(StatValues::new(Some(-1000), Some(4000), 3)),
                },
                ColumnSummary {
                    name: "msg".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::String(StatValues {
                        min: Some("msg a".into()),
                        max: Some("msg b".into()),
                        count: 3,
                        distinct_count: Some(NonZeroU64::new(3).unwrap()),
                    }),
                },
                ColumnSummary {
                    name: "temp".into(),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::F64(StatValues::new(Some(10.0), Some(30000.0), 3)),
                },
                ColumnSummary {
                    name: "time".into(),
                    influxdb_type: Some(InfluxDbType::Timestamp),
                    stats: Statistics::I64(StatValues::new(Some(3333), Some(11111111), 3)),
                },
            ],
        }];

        assert_eq!(
            expected, summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, summaries
        );
    }

    #[test]
    fn read_filter() {
        let mut chunk = Chunk::new("Coolverine", ChunkMetrics::new_unregistered());

        // Add a bunch of row groups to a single table in a single chunk
        for &i in &[100, 200, 300] {
            let schema = SchemaBuilder::new()
                .non_null_tag("env")
                .non_null_tag("region")
                .non_null_field("counter", Float64)
                .field("sketchy_sensor", Int64)
                .non_null_field("active", Boolean)
                .field("msg", Utf8)
                .timestamp()
                .build()
                .unwrap();

            let data: Vec<ArrayRef> = vec![
                Arc::new(
                    vec!["us-west", "us-east", "us-west"]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(
                    vec!["west", "west", "east"]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(Float64Array::from(vec![1.2, 300.3, 4500.3])),
                Arc::new(Int64Array::from(vec![None, Some(33), Some(44)])),
                Arc::new(BooleanArray::from(vec![true, false, false])),
                Arc::new(StringArray::from(vec![
                    Some("message a"),
                    Some("message b"),
                    None,
                ])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![i, 2 * i, 3 * i],
                    None,
                )),
            ];

            // Add a record batch to a single partition
            let rb = RecordBatch::try_new(schema.into(), data).unwrap();
            chunk.upsert_table("Coolverine", rb);
        }

        // Build the operation equivalent to the following query:
        //
        //   SELECT * FROM "table_1"
        //   WHERE "env" = 'us-west' AND
        //   "time" >= 100 AND  "time" < 205
        //
        let predicate =
            Predicate::with_time_range(&[BinaryExpr::from(("env", "=", "us-west"))], 100, 205); // filter on time

        let mut itr = chunk.read_filter("Coolverine", predicate, Selection::All);

        let exp_env_values = Values::Dictionary(vec![0], vec![Some("us-west")]);
        let exp_region_values = Values::Dictionary(vec![0], vec![Some("west")]);
        let exp_counter_values = Values::F64(vec![1.2]);
        let exp_sketchy_sensor_values = Values::I64N(vec![None]);
        let exp_active_values = Values::Bool(vec![Some(true)]);
        let exp_msg_values = Values::String(vec![Some("message a")]);

        let first_row_group = itr.next().unwrap();
        assert_rb_column_equals(&first_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&first_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&first_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&first_row_group, "msg", &exp_msg_values);
        assert_rb_column_equals(&first_row_group, "time", &Values::I64(vec![100])); // first row from first record batch

        let second_row_group = itr.next().unwrap();
        assert_rb_column_equals(&second_row_group, "env", &exp_env_values);
        assert_rb_column_equals(&second_row_group, "region", &exp_region_values);
        assert_rb_column_equals(&second_row_group, "counter", &exp_counter_values);
        assert_rb_column_equals(
            &first_row_group,
            "sketchy_sensor",
            &exp_sketchy_sensor_values,
        );
        assert_rb_column_equals(&first_row_group, "active", &exp_active_values);
        assert_rb_column_equals(&second_row_group, "time", &Values::I64(vec![200])); // first row from second record batch

        // No more data
        assert!(itr.next().is_none());
    }

    #[test]
    fn could_pass_predicate() {
        let mut chunk = Chunk::new("a_table", ChunkMetrics::new_unregistered());

        // Add table data to the chunk.
        chunk.upsert_table("a_table", gen_recordbatch());

        assert!(
            chunk.could_pass_predicate(Predicate::new(vec![BinaryExpr::from((
                "region", "=", "east"
            ))]))
        );
    }

    #[test]
    fn satisfies_predicate() {
        let columns = vec![
            (
                "time".to_owned(),
                ColumnType::create_time(&[1_i64, 2, 3, 4, 5, 6]),
            ),
            (
                "region".to_owned(),
                ColumnType::create_tag(&["west", "west", "east", "west", "south", "north"]),
            ),
        ];
        let rg = RowGroup::new(6, columns);

        let mut chunk = Chunk::new("table_1", ChunkMetrics::new_unregistered());
        chunk.table.add_row_group(rg);

        // No predicate so at least one row matches
        assert!(chunk.satisfies_predicate(&Predicate::default()));

        // at least one row satisfies the predicate
        assert!(
            chunk.satisfies_predicate(&Predicate::new(vec![BinaryExpr::from((
                "region", ">=", "west"
            ))]),)
        );

        // no rows match the predicate
        assert!(
            !chunk.satisfies_predicate(&Predicate::new(vec![BinaryExpr::from((
                "region", ">", "west"
            ))]),)
        );
    }

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn column_names() {
        let mut chunk = Chunk::new("Utopia", ChunkMetrics::new_unregistered());

        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_field("counter", Float64)
            .timestamp()
            .field("sketchy_sensor", Float64)
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["west", "west", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(Float64Array::from(vec![1.2, 3.3, 45.3])),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
            Arc::new(Float64Array::from(vec![Some(11.0), None, Some(12.0)])),
        ];

        // Add the above table to the chunk
        let rb = RecordBatch::try_new(schema, data).unwrap();
        chunk.upsert_table("Utopia", rb);

        let result = chunk
            .column_names(
                "Utopia",
                Predicate::default(),
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_set(&["counter", "region", "sketchy_sensor", "time"])
        );

        // Testing predicates
        let result = chunk
            .column_names(
                "Utopia",
                Predicate::new(vec![BinaryExpr::from(("time", "=", 222222_i64))]),
                Selection::All,
                BTreeSet::new(),
            )
            .unwrap();

        // sketchy_sensor won't be returned because it has a NULL value for the
        // only matching row.
        assert_eq!(result, to_set(&["counter", "region", "time"]));
    }

    fn to_map(arr: Vec<(&str, &[&str])>) -> BTreeMap<String, BTreeSet<String>> {
        arr.iter()
            .map(|(k, values)| {
                (
                    k.to_string(),
                    values
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<BTreeSet<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    }

    #[test]
    fn column_values() {
        let mut chunk = Chunk::new("my_table", ChunkMetrics::new_unregistered());

        let schema = SchemaBuilder::new()
            .non_null_tag("region")
            .non_null_tag("env")
            .timestamp()
            .build()
            .unwrap()
            .into();

        let data: Vec<ArrayRef> = vec![
            Arc::new(
                vec!["north", "south", "east"]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(
                vec![Some("prod"), None, Some("stag")]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ),
            Arc::new(TimestampNanosecondArray::from_vec(
                vec![11111111, 222222, 3333],
                None,
            )),
        ];

        // Add the above table to a chunk and partition
        let rb = RecordBatch::try_new(schema, data).unwrap();
        chunk.upsert_table("my_table", rb);

        let result = chunk
            .column_values(
                "my_table",
                Predicate::default(),
                Selection::Some(&["region", "env"]),
                BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_map(vec![
                ("region", &["north", "south", "east"]),
                ("env", &["prod", "stag"])
            ])
        );

        // With a predicate
        let result = chunk
            .column_values(
                "my_table",
                Predicate::new(vec![
                    BinaryExpr::from(("time", ">=", 20_i64)),
                    BinaryExpr::from(("time", "<=", 3333_i64)),
                ]),
                Selection::Some(&["region", "env"]),
                BTreeMap::new(),
            )
            .unwrap();

        assert_eq!(
            result,
            to_map(vec![
                ("region", &["east"]),
                ("env", &["stag"]) // column_values returns non-null values.
            ])
        );

        // Error when All column selection provided.
        assert!(matches!(
            chunk.column_values("x", Predicate::default(), Selection::All, BTreeMap::new()),
            Err(Error::UnsupportedOperation { .. })
        ));
    }
}
