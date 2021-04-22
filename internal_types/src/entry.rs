//! This module contains helper code for building `Entry` and `SequencedEntry`
//! from line protocol and the `DatabaseRules` configuration.

use crate::schema::TIME_COLUMN_NAME;
use data_types::{
    database_rules::{Error as DataError, Partitioner, ShardId, Sharder, WriterId},
    ClockValue,
};
use generated_types::entry as entry_fb;
use influxdb_line_protocol::{FieldValue, ParsedLine};

use std::{collections::BTreeMap, convert::TryFrom};

use chrono::{DateTime, Utc};
use flatbuffers::{FlatBufferBuilder, Follow, ForwardsUOffset, Vector, VectorIter, WIPOffset};
use ouroboros::self_referencing;
use snafu::{ResultExt, Snafu};
use std::fmt::Formatter;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error generating partition key {}", source))]
    GeneratingPartitionKey { source: DataError },

    #[snafu(display("Error getting shard id {}", source))]
    GeneratingShardId { source: DataError },

    #[snafu(display(
        "table {} has column {} {} with new data on line {}",
        table,
        column,
        source,
        line_number
    ))]
    TableColumnTypeMismatch {
        table: String,
        column: String,
        line_number: usize,
        source: ColumnError,
    },

    #[snafu(display("invalid flatbuffers: field {} is required", field))]
    FlatbufferFieldMissing { field: String },
}

#[derive(Debug, Snafu)]
pub enum ColumnError {
    #[snafu(display("type mismatch: expected {} but got {}", expected_type, new_type))]
    ColumnTypeMismatch {
        new_type: String,
        expected_type: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
type ColumnResult<T, E = ColumnError> = std::result::Result<T, E>;

/// Converts parsed line protocol into a collection of ShardedEntry with the
/// underlying flatbuffers bytes generated.
pub fn lines_to_sharded_entries(
    lines: &[ParsedLine<'_>],
    sharder: Option<&impl Sharder>,
    partitioner: &impl Partitioner,
) -> Result<Vec<ShardedEntry>> {
    let default_time = Utc::now();
    let mut sharded_lines = BTreeMap::new();

    for line in lines {
        let shard_id = match &sharder {
            Some(s) => Some(s.shard(line).context(GeneratingShardId)?),
            None => None,
        };
        let partition_key = partitioner
            .partition_key(line, &default_time)
            .context(GeneratingPartitionKey)?;
        let table = line.series.measurement.as_str();

        sharded_lines
            .entry(shard_id)
            .or_insert_with(BTreeMap::new)
            .entry(partition_key)
            .or_insert_with(BTreeMap::new)
            .entry(table)
            .or_insert_with(Vec::new)
            .push(line);
    }

    let default_time = Utc::now();

    let sharded_entries = sharded_lines
        .into_iter()
        .map(|(shard_id, partitions)| build_sharded_entry(shard_id, partitions, &default_time))
        .collect::<Result<Vec<_>>>()?;

    Ok(sharded_entries)
}

fn build_sharded_entry(
    shard_id: Option<ShardId>,
    partitions: BTreeMap<String, BTreeMap<&str, Vec<&ParsedLine<'_>>>>,
    default_time: &DateTime<Utc>,
) -> Result<ShardedEntry> {
    let mut fbb = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

    let partition_writes = partitions
        .into_iter()
        .map(|(partition_key, tables)| {
            build_partition_write(&mut fbb, partition_key, tables, default_time)
        })
        .collect::<Result<Vec<_>>>()?;
    let partition_writes = fbb.create_vector(&partition_writes);

    let write_operations = entry_fb::WriteOperations::create(
        &mut fbb,
        &entry_fb::WriteOperationsArgs {
            partition_writes: Some(partition_writes),
        },
    );
    let entry = entry_fb::Entry::create(
        &mut fbb,
        &entry_fb::EntryArgs {
            operation_type: entry_fb::Operation::write,
            operation: Some(write_operations.as_union_value()),
        },
    );

    fbb.finish(entry, None);

    let (mut data, idx) = fbb.collapse();
    let entry = Entry::try_from(data.split_off(idx))
        .expect("Flatbuffer data just constructed should be valid");

    Ok(ShardedEntry { shard_id, entry })
}

fn build_partition_write<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    partition_key: String,
    tables: BTreeMap<&str, Vec<&'a ParsedLine<'_>>>,
    default_time: &DateTime<Utc>,
) -> Result<flatbuffers::WIPOffset<entry_fb::PartitionWrite<'a>>> {
    let partition_key = fbb.create_string(&partition_key);

    let table_batches = tables
        .into_iter()
        .map(|(table_name, lines)| build_table_write_batch(fbb, table_name, lines, default_time))
        .collect::<Result<Vec<_>>>()?;
    let table_batches = fbb.create_vector(&table_batches);

    Ok(entry_fb::PartitionWrite::create(
        fbb,
        &entry_fb::PartitionWriteArgs {
            key: Some(partition_key),
            table_batches: Some(table_batches),
        },
    ))
}

fn build_table_write_batch<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    table_name: &str,
    lines: Vec<&'a ParsedLine<'_>>,
    default_time: &DateTime<Utc>,
) -> Result<flatbuffers::WIPOffset<entry_fb::TableWriteBatch<'a>>> {
    let mut columns = BTreeMap::new();
    for (i, line) in lines.iter().enumerate() {
        let row_number = i + 1;

        if let Some(tagset) = &line.series.tag_set {
            for (key, value) in tagset {
                let key = key.as_str();
                let builder = columns
                    .entry(key)
                    .or_insert_with(ColumnBuilder::new_tag_column);
                builder.null_to_row(row_number);
                builder
                    .push_tag(value.as_str())
                    .context(TableColumnTypeMismatch {
                        table: table_name,
                        column: key,
                        line_number: i,
                    })?;
            }
        }

        for (key, val) in &line.field_set {
            let key = key.as_str();

            match val {
                FieldValue::Boolean(b) => {
                    let builder = columns
                        .entry(key)
                        .or_insert_with(ColumnBuilder::new_bool_column);
                    builder.null_to_row(row_number);
                    builder.push_bool(*b).context(TableColumnTypeMismatch {
                        table: table_name,
                        column: key,
                        line_number: i,
                    })?;
                }
                FieldValue::U64(v) => {
                    let builder = columns
                        .entry(key)
                        .or_insert_with(ColumnBuilder::new_u64_column);
                    builder.null_to_row(row_number);
                    builder.push_u64(*v).context(TableColumnTypeMismatch {
                        table: table_name,
                        column: key,
                        line_number: i,
                    })?;
                }
                FieldValue::F64(v) => {
                    let builder = columns
                        .entry(key)
                        .or_insert_with(ColumnBuilder::new_f64_column);
                    builder.null_to_row(row_number);
                    builder.push_f64(*v).context(TableColumnTypeMismatch {
                        table: table_name,
                        column: key,
                        line_number: i,
                    })?;
                }
                FieldValue::I64(v) => {
                    let builder = columns
                        .entry(key)
                        .or_insert_with(ColumnBuilder::new_i64_column);
                    builder.null_to_row(row_number);
                    builder.push_i64(*v).context(TableColumnTypeMismatch {
                        table: table_name,
                        column: key,
                        line_number: i,
                    })?;
                }
                FieldValue::String(v) => {
                    let builder = columns
                        .entry(key)
                        .or_insert_with(ColumnBuilder::new_string_column);
                    builder.null_to_row(row_number);
                    builder
                        .push_string(v.as_str())
                        .context(TableColumnTypeMismatch {
                            table: table_name,
                            column: key,
                            line_number: i,
                        })?;
                }
            }
        }

        let builder = columns
            .entry(TIME_COLUMN_NAME)
            .or_insert_with(ColumnBuilder::new_time_column);
        builder
            .push_time(
                line.timestamp
                    .unwrap_or_else(|| default_time.timestamp_nanos()),
            )
            .context(TableColumnTypeMismatch {
                table: table_name,
                column: TIME_COLUMN_NAME,
                line_number: i,
            })?;

        for b in columns.values_mut() {
            b.null_to_row(row_number + 1);
        }
    }

    let columns = columns
        .into_iter()
        .map(|(column_name, builder)| builder.build_flatbuffer(fbb, column_name))
        .collect::<Vec<_>>();
    let columns = fbb.create_vector(&columns);

    let table_name = fbb.create_string(table_name);

    Ok(entry_fb::TableWriteBatch::create(
        fbb,
        &entry_fb::TableWriteBatchArgs {
            name: Some(table_name),
            columns: Some(columns),
        },
    ))
}

/// Holds a shard id to the associated entry. If there is no ShardId, then
/// everything goes to the same place. This means a single entry will be
/// generated from a batch of line protocol.
#[derive(Debug)]
pub struct ShardedEntry {
    pub shard_id: Option<ShardId>,
    pub entry: Entry,
}

/// Wrapper type for the flatbuffer Entry struct. Has convenience methods for
/// iterating through the partitioned writes.
#[self_referencing]
#[derive(Debug, PartialEq)]
pub struct Entry {
    data: Vec<u8>,
    #[borrows(data)]
    #[covariant]
    fb: entry_fb::Entry<'this>,
}

impl Entry {
    /// Returns the Flatbuffers struct for the Entry
    pub fn fb(&self) -> &entry_fb::Entry<'_> {
        self.borrow_fb()
    }

    /// Returns the serialized bytes for the Entry
    pub fn data(&self) -> &[u8] {
        self.borrow_data()
    }

    pub fn partition_writes(&self) -> Option<Vec<PartitionWrite<'_>>> {
        match self.fb().operation_as_write().as_ref() {
            Some(w) => w
                .partition_writes()
                .as_ref()
                .map(|w| w.iter().map(|fb| PartitionWrite { fb }).collect::<Vec<_>>()),
            None => None,
        }
    }
}

impl TryFrom<Vec<u8>> for Entry {
    type Error = flatbuffers::InvalidFlatbuffer;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        EntryTryBuilder {
            data,
            fb_builder: |data| flatbuffers::root::<entry_fb::Entry<'_>>(data),
        }
        .try_build()
    }
}

/// Wrapper struct for the flatbuffers PartitionWrite. Has convenience methods
/// for iterating through the table batches.
#[derive(Debug)]
pub struct PartitionWrite<'a> {
    fb: entry_fb::PartitionWrite<'a>,
}

impl<'a> PartitionWrite<'a> {
    pub fn key(&self) -> &str {
        self.fb
            .key()
            .expect("key must be present in the flatbuffer PartitionWrite")
    }

    pub fn table_batches(&self) -> Vec<TableBatch<'_>> {
        match self.fb.table_batches().as_ref() {
            Some(batches) => batches
                .iter()
                .map(|fb| TableBatch { fb })
                .collect::<Vec<_>>(),
            None => vec![],
        }
    }
}

/// Wrapper struct for the flatbuffers TableBatch. Has convenience methods for
/// iterating through the data in columnar format.
#[derive(Debug)]
pub struct TableBatch<'a> {
    fb: entry_fb::TableWriteBatch<'a>,
}

impl<'a> TableBatch<'a> {
    pub fn name(&self) -> &str {
        self.fb
            .name()
            .expect("name must be present in flatbuffers TableWriteBatch")
    }

    pub fn columns(&self) -> Vec<Column<'_>> {
        match self.fb.columns().as_ref() {
            Some(columns) => {
                let row_count = self.row_count();
                columns
                    .iter()
                    .map(|fb| Column { fb, row_count })
                    .collect::<Vec<_>>()
            }
            None => vec![],
        }
    }

    pub fn row_count(&self) -> usize {
        if let Some(cols) = self.fb.columns() {
            if let Some(c) = cols.iter().next() {
                let null_count = match c.null_mask() {
                    Some(m) => m.iter().map(|b| b.count_ones() as usize).sum(),
                    None => 0,
                };

                let value_count = match c.values_type() {
                    entry_fb::ColumnValues::BoolValues => {
                        c.values_as_bool_values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::U64Values => {
                        c.values_as_u64values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::F64Values => {
                        c.values_as_f64values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::I64Values => {
                        c.values_as_i64values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::StringValues => {
                        c.values_as_string_values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::BytesValues => {
                        c.values_as_bytes_values().unwrap().values().unwrap().len()
                    }
                    _ => panic!("invalid column flatbuffers"),
                };

                return value_count + null_count;
            }
        }

        0
    }
}

/// Wrapper struct for the flatbuffers Column. Has a convenience method to
/// return an iterator for the values in the column.
#[derive(Debug)]
pub struct Column<'a> {
    fb: entry_fb::Column<'a>,
    pub row_count: usize,
}

impl<'a> Column<'a> {
    pub fn name(&self) -> &str {
        self.fb
            .name()
            .expect("name must be present in flatbuffers Column")
    }

    pub fn logical_type(&self) -> entry_fb::LogicalColumnType {
        self.fb.logical_column_type()
    }

    pub fn is_tag(&self) -> bool {
        self.fb.logical_column_type() == entry_fb::LogicalColumnType::Tag
    }

    pub fn is_field(&self) -> bool {
        self.fb.logical_column_type() == entry_fb::LogicalColumnType::Field
    }

    pub fn is_time(&self) -> bool {
        self.fb.logical_column_type() == entry_fb::LogicalColumnType::Time
    }

    pub fn values(&self) -> TypedValuesIterator<'a> {
        match self.fb.values_type() {
            entry_fb::ColumnValues::BoolValues => TypedValuesIterator::Bool(BoolIterator {
                row_count: self.row_count,
                position: 0,
                null_mask: self.fb.null_mask(),
                value_position: 0,
                values: self
                    .fb
                    .values_as_bool_values()
                    .expect("invalid flatbuffers")
                    .values()
                    .unwrap_or(&[]),
            }),
            entry_fb::ColumnValues::StringValues => {
                let values = self
                    .fb
                    .values_as_string_values()
                    .expect("invalid flatbuffers")
                    .values()
                    .expect("flatbuffers StringValues must have string values set")
                    .iter();

                TypedValuesIterator::String(StringIterator {
                    row_count: self.row_count,
                    position: 0,
                    null_mask: self.fb.null_mask(),
                    values,
                })
            }
            entry_fb::ColumnValues::I64Values => {
                let values_iter = self
                    .fb
                    .values_as_i64values()
                    .expect("invalid flatbuffers")
                    .values()
                    .unwrap_or_else(|| Vector::new(&[], 0))
                    .iter();

                TypedValuesIterator::I64(ValIterator {
                    row_count: self.row_count,
                    position: 0,
                    null_mask: self.fb.null_mask(),
                    values_iter,
                })
            }
            entry_fb::ColumnValues::F64Values => {
                let values_iter = self
                    .fb
                    .values_as_f64values()
                    .expect("invalid flatbuffers")
                    .values()
                    .unwrap_or_else(|| Vector::new(&[], 0))
                    .iter();

                TypedValuesIterator::F64(ValIterator {
                    row_count: self.row_count,
                    position: 0,
                    null_mask: self.fb.null_mask(),
                    values_iter,
                })
            }
            entry_fb::ColumnValues::U64Values => {
                let values_iter = self
                    .fb
                    .values_as_u64values()
                    .expect("invalid flatbuffers")
                    .values()
                    .unwrap_or_else(|| Vector::new(&[], 0))
                    .iter();

                TypedValuesIterator::U64(ValIterator {
                    row_count: self.row_count,
                    position: 0,
                    null_mask: self.fb.null_mask(),
                    values_iter,
                })
            }
            entry_fb::ColumnValues::BytesValues => unimplemented!(),
            _ => panic!("unknown fb values type"),
        }
    }
}

/// Wrapper for the iterators for the underlying column types.
#[derive(Debug)]
pub enum TypedValuesIterator<'a> {
    Bool(BoolIterator<'a>),
    I64(ValIterator<'a, i64>),
    F64(ValIterator<'a, f64>),
    U64(ValIterator<'a, u64>),
    String(StringIterator<'a>),
}

impl<'a> TypedValuesIterator<'a> {
    pub fn bool_values(self) -> Option<Vec<Option<bool>>> {
        match self {
            Self::Bool(b) => Some(b.collect::<Vec<_>>()),
            _ => None,
        }
    }

    pub fn i64_values(self) -> Option<Vec<Option<i64>>> {
        match self {
            Self::I64(v) => Some(v.collect::<Vec<_>>()),
            _ => None,
        }
    }

    pub fn f64_values(self) -> Option<Vec<Option<f64>>> {
        match self {
            Self::F64(v) => Some(v.collect::<Vec<_>>()),
            _ => None,
        }
    }

    pub fn u64_values(self) -> Option<Vec<Option<u64>>> {
        match self {
            Self::U64(v) => Some(v.collect::<Vec<_>>()),
            _ => None,
        }
    }

    pub fn type_description(&self) -> &str {
        match self {
            Self::Bool(_) => "bool",
            Self::I64(_) => "i64",
            Self::F64(_) => "f64",
            Self::U64(_) => "u64",
            Self::String(_) => "String",
        }
    }
}

/// Iterator over the flatbuffers BoolValues
#[derive(Debug)]
pub struct BoolIterator<'a> {
    pub row_count: usize,
    position: usize,
    null_mask: Option<&'a [u8]>,
    values: &'a [bool],
    value_position: usize,
}

impl<'a> Iterator for BoolIterator<'a> {
    type Item = Option<bool>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.row_count || self.value_position >= self.values.len() {
            return None;
        }

        self.position += 1;
        if is_null_value(self.position, &self.null_mask) {
            return Some(None);
        }

        let val = Some(self.values[self.value_position]);
        self.value_position += 1;

        Some(val)
    }
}

/// Iterator over the flatbuffers I64Values, F64Values, and U64Values.
#[derive(Debug)]
pub struct ValIterator<'a, T: Follow<'a> + Follow<'a, Inner = T>> {
    pub row_count: usize,
    position: usize,
    null_mask: Option<&'a [u8]>,
    values_iter: VectorIter<'a, T>,
}

impl<'a, T: Follow<'a> + Follow<'a, Inner = T>> Iterator for ValIterator<'a, T> {
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.row_count {
            return None;
        }

        self.position += 1;
        if is_null_value(self.position, &self.null_mask) {
            return Some(None);
        }

        Some(self.values_iter.next())
    }
}

/// Iterator over the flatbuffers StringValues
#[derive(Debug)]
pub struct StringIterator<'a> {
    pub row_count: usize,
    position: usize,
    null_mask: Option<&'a [u8]>,
    values: VectorIter<'a, ForwardsUOffset<&'a str>>,
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.row_count {
            return None;
        }

        self.position += 1;
        if is_null_value(self.position, &self.null_mask) {
            return Some(None);
        }

        Some(self.values.next())
    }
}

struct NullMaskBuilder {
    bytes: Vec<u8>,
    position: usize,
}

const BITS_IN_BYTE: usize = 8;
const LEFT_MOST_BIT_TRUE: u8 = 128;

impl NullMaskBuilder {
    fn new() -> Self {
        Self {
            bytes: vec![0],
            position: 1,
        }
    }

    fn push(&mut self, is_null: bool) {
        if self.position > BITS_IN_BYTE {
            self.bytes.push(0);
            self.position = 1;
        }

        if is_null {
            let val: u8 = LEFT_MOST_BIT_TRUE >> (self.position - 1);
            let last_byte_position = self.bytes.len() - 1;
            self.bytes[last_byte_position] += val;
        }

        self.position += 1;
    }

    #[allow(dead_code)]
    fn to_bool_vec(&self) -> Vec<bool> {
        (1..self.row_count() + 1)
            .map(|r| is_null_value(r, &Some(&self.bytes)))
            .collect::<Vec<_>>()
    }

    fn row_count(&self) -> usize {
        self.bytes.len() * BITS_IN_BYTE - BITS_IN_BYTE + self.position - 1
    }

    fn has_nulls(&self) -> bool {
        for b in &self.bytes {
            if *b > 0 {
                return true;
            }
        }

        false
    }
}

impl std::fmt::Debug for NullMaskBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for i in 1..self.row_count() {
            let bit = if is_null_value(i, &Some(&self.bytes)) {
                1
            } else {
                0
            };

            write!(f, "{}", bit)?;
            if i % 4 == 0 {
                write!(f, " ")?;
            }
        }

        Ok(())
    }
}

fn is_null_value(row: usize, mask: &Option<&[u8]>) -> bool {
    match mask {
        Some(mask) => {
            let mut position = (row % BITS_IN_BYTE) as u8;
            let mut byte = row / BITS_IN_BYTE;

            if position == 0 {
                byte -= 1;
                position = BITS_IN_BYTE as u8;
            }

            if byte >= mask.len() {
                return true;
            }

            mask[byte] & (LEFT_MOST_BIT_TRUE >> (position - 1)) > 0
        }
        None => false,
    }
}

#[derive(Debug)]
struct ColumnBuilder<'a> {
    nulls: NullMaskBuilder,
    values: ColumnRaw<'a>,
}

impl<'a> ColumnBuilder<'a> {
    fn new_tag_column() -> Self {
        Self {
            nulls: NullMaskBuilder::new(),
            values: ColumnRaw::Tag(Vec::new()),
        }
    }

    fn new_string_column() -> Self {
        Self {
            nulls: NullMaskBuilder::new(),
            values: ColumnRaw::String(Vec::new()),
        }
    }

    fn new_time_column() -> Self {
        Self {
            nulls: NullMaskBuilder::new(),
            values: ColumnRaw::Time(Vec::new()),
        }
    }

    fn new_bool_column() -> Self {
        Self {
            nulls: NullMaskBuilder::new(),
            values: ColumnRaw::Bool(Vec::new()),
        }
    }

    fn new_u64_column() -> Self {
        Self {
            nulls: NullMaskBuilder::new(),
            values: ColumnRaw::U64(Vec::new()),
        }
    }

    fn new_f64_column() -> Self {
        Self {
            nulls: NullMaskBuilder::new(),
            values: ColumnRaw::F64(Vec::new()),
        }
    }

    fn new_i64_column() -> Self {
        Self {
            nulls: NullMaskBuilder::new(),
            values: ColumnRaw::I64(Vec::new()),
        }
    }

    // ensures there are at least as many rows (or nulls) to row_number - 1
    fn null_to_row(&mut self, row_number: usize) {
        let mut row_count = self.nulls.row_count();

        while row_count < row_number - 1 {
            self.nulls.push(true);
            row_count += 1;
        }
    }

    fn push_tag(&mut self, value: &'a str) -> ColumnResult<()> {
        match &mut self.values {
            ColumnRaw::Tag(values) => {
                self.nulls.push(false);
                values.push(value)
            }
            _ => {
                return ColumnTypeMismatch {
                    new_type: "tag",
                    expected_type: self.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    fn push_string(&mut self, value: &'a str) -> ColumnResult<()> {
        match &mut self.values {
            ColumnRaw::String(values) => {
                self.nulls.push(false);
                values.push(value)
            }
            _ => {
                return ColumnTypeMismatch {
                    new_type: "string",
                    expected_type: self.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    fn push_time(&mut self, value: i64) -> ColumnResult<()> {
        match &mut self.values {
            ColumnRaw::Time(times) => {
                times.push(value);
                self.nulls.push(false);
            }
            _ => {
                return ColumnTypeMismatch {
                    new_type: "time",
                    expected_type: self.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    fn push_bool(&mut self, value: bool) -> ColumnResult<()> {
        match &mut self.values {
            ColumnRaw::Bool(values) => {
                values.push(value);
                self.nulls.push(false);
            }
            _ => {
                return ColumnTypeMismatch {
                    new_type: "bool",
                    expected_type: self.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    fn push_u64(&mut self, value: u64) -> ColumnResult<()> {
        match &mut self.values {
            ColumnRaw::U64(values) => {
                values.push(value);
                self.nulls.push(false);
            }
            _ => {
                return ColumnTypeMismatch {
                    new_type: "u64",
                    expected_type: self.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    fn push_f64(&mut self, value: f64) -> ColumnResult<()> {
        match &mut self.values {
            ColumnRaw::F64(values) => {
                values.push(value);
                self.nulls.push(false);
            }
            _ => {
                return ColumnTypeMismatch {
                    new_type: "f64",
                    expected_type: self.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    fn push_i64(&mut self, value: i64) -> ColumnResult<()> {
        match &mut self.values {
            ColumnRaw::I64(values) => {
                values.push(value);
                self.nulls.push(false);
            }
            _ => {
                return ColumnTypeMismatch {
                    new_type: "i64",
                    expected_type: self.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    fn build_flatbuffer(
        &self,
        fbb: &mut FlatBufferBuilder<'a>,
        column_name: &str,
    ) -> WIPOffset<entry_fb::Column<'a>> {
        let name = Some(fbb.create_string(column_name));
        let null_mask = if self.nulls.has_nulls() {
            Some(fbb.create_vector_direct(&self.nulls.bytes))
        } else {
            None
        };

        let (logical_column_type, values_type, values) = match &self.values {
            ColumnRaw::Tag(values) => {
                let values = values
                    .iter()
                    .map(|v| fbb.create_string(v))
                    .collect::<Vec<_>>();
                let values = fbb.create_vector(&values);
                let values = entry_fb::StringValues::create(
                    fbb,
                    &entry_fb::StringValuesArgs {
                        values: Some(values),
                    },
                );

                (
                    entry_fb::LogicalColumnType::Tag,
                    entry_fb::ColumnValues::StringValues,
                    values.as_union_value(),
                )
            }
            ColumnRaw::String(values) => {
                let values = values
                    .iter()
                    .map(|v| fbb.create_string(v))
                    .collect::<Vec<_>>();
                let values = fbb.create_vector(&values);
                let values = entry_fb::StringValues::create(
                    fbb,
                    &entry_fb::StringValuesArgs {
                        values: Some(values),
                    },
                );

                (
                    entry_fb::LogicalColumnType::Field,
                    entry_fb::ColumnValues::StringValues,
                    values.as_union_value(),
                )
            }
            ColumnRaw::Time(values) => {
                let values = fbb.create_vector(&values);
                let values = entry_fb::I64Values::create(
                    fbb,
                    &entry_fb::I64ValuesArgs {
                        values: Some(values),
                    },
                );

                (
                    entry_fb::LogicalColumnType::Time,
                    entry_fb::ColumnValues::I64Values,
                    values.as_union_value(),
                )
            }
            ColumnRaw::I64(values) => {
                let values = fbb.create_vector(&values);
                let values = entry_fb::I64Values::create(
                    fbb,
                    &entry_fb::I64ValuesArgs {
                        values: Some(values),
                    },
                );

                (
                    entry_fb::LogicalColumnType::Field,
                    entry_fb::ColumnValues::I64Values,
                    values.as_union_value(),
                )
            }
            ColumnRaw::Bool(values) => {
                let values = fbb.create_vector(&values);
                let values = entry_fb::BoolValues::create(
                    fbb,
                    &entry_fb::BoolValuesArgs {
                        values: Some(values),
                    },
                );

                (
                    entry_fb::LogicalColumnType::Field,
                    entry_fb::ColumnValues::BoolValues,
                    values.as_union_value(),
                )
            }
            ColumnRaw::F64(values) => {
                let values = fbb.create_vector(&values);
                let values = entry_fb::F64Values::create(
                    fbb,
                    &entry_fb::F64ValuesArgs {
                        values: Some(values),
                    },
                );

                (
                    entry_fb::LogicalColumnType::Field,
                    entry_fb::ColumnValues::F64Values,
                    values.as_union_value(),
                )
            }
            ColumnRaw::U64(values) => {
                let values = fbb.create_vector(&values);
                let values = entry_fb::U64Values::create(
                    fbb,
                    &entry_fb::U64ValuesArgs {
                        values: Some(values),
                    },
                );

                (
                    entry_fb::LogicalColumnType::Field,
                    entry_fb::ColumnValues::U64Values,
                    values.as_union_value(),
                )
            }
        };

        entry_fb::Column::create(
            fbb,
            &entry_fb::ColumnArgs {
                name,
                logical_column_type,
                values_type,
                values: Some(values),
                null_mask,
            },
        )
    }

    fn type_description(&self) -> &str {
        match self.values {
            ColumnRaw::String(_) => "string",
            ColumnRaw::I64(_) => "i64",
            ColumnRaw::F64(_) => "f64",
            ColumnRaw::U64(_) => "u64",
            ColumnRaw::Time(_) => "time",
            ColumnRaw::Tag(_) => "tag",
            ColumnRaw::Bool(_) => "bool",
        }
    }
}

#[derive(Debug)]
enum ColumnRaw<'a> {
    Tag(Vec<&'a str>),
    Time(Vec<i64>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    U64(Vec<u64>),
    String(Vec<&'a str>),
    Bool(Vec<bool>),
}

#[self_referencing]
#[derive(Debug)]
pub struct SequencedEntry {
    data: Vec<u8>,
    #[borrows(data)]
    #[covariant]
    fb: entry_fb::SequencedEntry<'this>,
    #[borrows(data)]
    #[covariant]
    entry: Option<entry_fb::Entry<'this>>,
}

impl SequencedEntry {
    pub fn size(&self) -> usize {
        self.borrow_data().len()
    }

    pub fn new_from_entry_bytes(
        clock_value: ClockValue,
        writer_id: u32,
        entry_bytes: &[u8],
    ) -> Result<Self> {
        // The flatbuffer contains:
        //    1xu64 -> clock_value
        //    1xu32 -> writer_id
        //    0?       -> entry (unused here)
        //    input   -> entry_bytes
        // The buffer also needs space for the flatbuffer vtable.
        const OVERHEAD: usize = 4 * std::mem::size_of::<u64>();
        let mut fbb = FlatBufferBuilder::new_with_capacity(entry_bytes.len() + OVERHEAD);

        let entry_bytes = fbb.create_vector_direct(entry_bytes);
        let sequenced_entry = entry_fb::SequencedEntry::create(
            &mut fbb,
            &entry_fb::SequencedEntryArgs {
                clock_value: clock_value.get(),
                writer_id,
                entry_bytes: Some(entry_bytes),
            },
        );

        fbb.finish(sequenced_entry, None);

        let (mut data, idx) = fbb.collapse();
        let sequenced_entry = Self::try_from(data.split_off(idx))
            .expect("Flatbuffer data just constructed should be valid");

        Ok(sequenced_entry)
    }

    /// Returns the Flatbuffers struct for the SequencedEntry
    pub fn fb(&self) -> &entry_fb::SequencedEntry<'_> {
        self.borrow_fb()
    }

    pub fn partition_writes(&self) -> Option<Vec<PartitionWrite<'_>>> {
        match self.borrow_entry().as_ref() {
            Some(e) => match e.operation_as_write().as_ref() {
                Some(w) => w
                    .partition_writes()
                    .as_ref()
                    .map(|w| w.iter().map(|fb| PartitionWrite { fb }).collect::<Vec<_>>()),
                None => None,
            },
            None => None,
        }
    }

    pub fn clock_value(&self) -> ClockValue {
        ClockValue::new(self.fb().clock_value())
    }

    pub fn writer_id(&self) -> WriterId {
        self.fb().writer_id()
    }
}

impl TryFrom<Vec<u8>> for SequencedEntry {
    type Error = flatbuffers::InvalidFlatbuffer;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        SequencedEntryTryBuilder {
            data,
            fb_builder: |data| flatbuffers::root::<entry_fb::SequencedEntry<'_>>(data),
            entry_builder: |data| match flatbuffers::root::<entry_fb::SequencedEntry<'_>>(data)?
                .entry_bytes()
            {
                Some(entry_bytes) => Ok(Some(flatbuffers::root::<entry_fb::Entry<'_>>(
                    &entry_bytes,
                )?)),
                None => Ok(None),
            },
        }
        .try_build()
    }
}

pub mod test_helpers {
    use super::*;
    use chrono::TimeZone;
    use influxdb_line_protocol::parse_lines;

    // An appropriate maximum size for batches of LP to be written into IOx. Using
    // test fixtures containing more than this many lines of LP will result in them
    // being written as multiple writes.
    const LP_BATCH_SIZE: usize = 10000;

    /// Converts the line protocol to a single `Entry` with a single shard and
    /// a single partition.
    pub fn lp_to_entry(lp: &str) -> Entry {
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        lines_to_sharded_entries(&lines, sharder(1).as_ref(), &hour_partitioner())
            .unwrap()
            .pop()
            .unwrap()
            .entry
    }

    /// Converts the line protocol to a collection of `Entry` with a single
    /// shard and a single partition, which is useful for testing when `lp` is
    /// large. Batches are sized according to LP_BATCH_SIZE.
    pub fn lp_to_entries(lp: &str) -> Vec<Entry> {
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        lines
            .chunks(LP_BATCH_SIZE)
            .map(|batch| {
                lines_to_sharded_entries(batch, sharder(1).as_ref(), &hour_partitioner())
                    .unwrap()
                    .pop()
                    .unwrap()
                    .entry
            })
            .collect::<Vec<_>>()
    }

    /// Returns a test sharder that will assign shard ids from [0, count)
    /// incrementing for each line.
    pub fn sharder(count: ShardId) -> Option<TestSharder> {
        Some(TestSharder {
            count,
            n: std::cell::RefCell::new(0),
        })
    }

    // For each line passed to shard returns a shard id from [0, count) in order
    #[derive(Debug)]
    pub struct TestSharder {
        count: ShardId,
        n: std::cell::RefCell<ShardId>,
    }

    impl Sharder for TestSharder {
        fn shard(&self, _line: &ParsedLine<'_>) -> Result<ShardId, DataError> {
            let n = *self.n.borrow();
            self.n.replace(n + 1);
            Ok(n % self.count)
        }
    }

    /// Returns a test partitioner that will partition data by the hour
    pub fn hour_partitioner() -> HourPartitioner {
        HourPartitioner {}
    }

    /// Returns a test partitioner that will assign partition keys in the form
    /// key_# where # is replaced by a number `[0, count)` incrementing for
    /// each line.
    pub fn partitioner(count: u8) -> TestPartitioner {
        TestPartitioner {
            count,
            n: std::cell::RefCell::new(0),
        }
    }

    // For each line passed to partition_key returns a key with a number from
    // `[0, count)`
    #[derive(Debug)]
    pub struct TestPartitioner {
        count: u8,
        n: std::cell::RefCell<u8>,
    }

    impl Partitioner for TestPartitioner {
        fn partition_key(
            &self,
            _line: &ParsedLine<'_>,
            _default_time: &DateTime<Utc>,
        ) -> data_types::database_rules::Result<String> {
            let n = *self.n.borrow();
            self.n.replace(n + 1);
            Ok(format!("key_{}", n % self.count))
        }
    }

    // Partitions by the hour
    #[derive(Debug)]
    pub struct HourPartitioner {}

    impl Partitioner for HourPartitioner {
        fn partition_key(
            &self,
            line: &ParsedLine<'_>,
            default_time: &DateTime<Utc>,
        ) -> data_types::database_rules::Result<String> {
            const HOUR_FORMAT: &str = "%Y-%m-%dT%H";

            let key = match line.timestamp {
                Some(t) => Utc.timestamp_nanos(t).format(HOUR_FORMAT),
                None => default_time.format(HOUR_FORMAT),
            }
            .to_string();

            Ok(key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use super::*;
    use data_types::database_rules::NO_SHARD_CONFIG;
    use influxdb_line_protocol::parse_lines;

    #[test]
    fn shards_lines() {
        let lp = vec![
            "cpu,host=a,region=west user=23.1,system=66.1 123",
            "mem,host=a,region=west used=23432 123",
            "foo bar=true 21",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(2).as_ref(), &partitioner(1)).unwrap();

        assert_eq!(sharded_entries.len(), 2);
        assert_eq!(sharded_entries[0].shard_id, Some(0));
        assert_eq!(sharded_entries[1].shard_id, Some(1));
    }

    #[test]
    fn no_shard_config() {
        let lp = vec![
            "cpu,host=a,region=west user=23.1,system=66.1 123",
            "mem,host=a,region=west used=23432 123",
            "foo bar=true 21",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, NO_SHARD_CONFIG, &partitioner(1)).unwrap();

        assert_eq!(sharded_entries.len(), 1);
        assert_eq!(sharded_entries[0].shard_id, None);
    }

    #[test]
    fn multiple_partitions() {
        let lp = vec![
            "cpu,host=a,region=west user=23.1,system=66.1 123",
            "mem,host=a,region=west used=23432 123",
            "asdf foo=\"bar\" 9999",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(2)).unwrap();

        let partition_writes = sharded_entries[0].entry.partition_writes().unwrap();
        assert_eq!(partition_writes.len(), 2);
        assert_eq!(partition_writes[0].key(), "key_0");
        assert_eq!(partition_writes[1].key(), "key_1");
    }

    #[test]
    fn multiple_tables() {
        let lp = vec![
            "cpu val=1 55",
            "mem val=23 10",
            "cpu val=88 100",
            "disk foo=23.2 110",
            "mem val=55 111",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries[0].entry.partition_writes().unwrap();
        let table_batches = partition_writes[0].table_batches();

        assert_eq!(table_batches.len(), 3);
        assert_eq!(table_batches[0].name(), "cpu");
        assert_eq!(table_batches[1].name(), "disk");
        assert_eq!(table_batches[2].name(), "mem");
    }

    #[test]
    fn logical_column_types() {
        let lp = vec!["a,host=a val=23 983", "a,host=a,region=west val2=23.2 2343"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries[0].entry.partition_writes().unwrap();
        let table_batches = partition_writes[0].table_batches();
        let batch = &table_batches[0];

        let columns = batch.columns();

        assert_eq!(columns.len(), 5);

        assert_eq!(columns[0].name(), "host");
        assert_eq!(columns[0].logical_type(), entry_fb::LogicalColumnType::Tag);

        assert_eq!(columns[1].name(), "region");
        assert_eq!(columns[1].logical_type(), entry_fb::LogicalColumnType::Tag);

        assert_eq!(columns[2].name(), "time");
        assert_eq!(columns[2].logical_type(), entry_fb::LogicalColumnType::Time);

        assert_eq!(columns[3].name(), "val");
        assert_eq!(
            columns[3].logical_type(),
            entry_fb::LogicalColumnType::Field
        );

        assert_eq!(columns[4].name(), "val2");
        assert_eq!(
            columns[4].logical_type(),
            entry_fb::LogicalColumnType::Field
        );
    }

    #[test]
    fn columns_without_nulls() {
        let lp = vec![
            "a,host=a ival=23i,fval=1.2,uval=7u,sval=\"hi\",bval=true 1",
            "a,host=b ival=22i,fval=2.2,uval=1u,sval=\"world\",bval=false 2",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();

        let columns = batch.columns();

        assert_eq!(batch.row_count(), 2);
        assert_eq!(columns.len(), 7);

        let col = columns.get(0).unwrap();
        assert_eq!(col.name(), "bval");
        let values = col.values().bool_values().unwrap();
        assert_eq!(&values, &[Some(true), Some(false)]);

        let col = columns.get(1).unwrap();
        assert_eq!(col.name(), "fval");
        let values = col.values().f64_values().unwrap();
        assert_eq!(&values, &[Some(1.2), Some(2.2)]);

        let col = columns.get(2).unwrap();
        assert_eq!(col.name(), "host");
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[Some("a"), Some("b")]);

        let col = columns.get(3).unwrap();
        assert_eq!(col.name(), "ival");
        let values = col.values().i64_values().unwrap();
        assert_eq!(&values, &[Some(23), Some(22)]);

        let col = columns.get(4).unwrap();
        assert_eq!(col.name(), "sval");
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[Some("hi"), Some("world")]);

        let col = columns.get(5).unwrap();
        assert_eq!(col.name(), TIME_COLUMN_NAME);
        let values = col.values().i64_values().unwrap();
        assert_eq!(&values, &[Some(1), Some(2)]);

        let col = columns.get(6).unwrap();
        assert_eq!(col.name(), "uval");
        let values = col.values().u64_values().unwrap();
        assert_eq!(&values, &[Some(7), Some(1)]);
    }

    #[test]
    fn columns_with_nulls() {
        let lp = vec![
            "a,host=a val=23i 983",
            "a,host=a,region=west val2=23.2 2343",
            "a val=21i,bool=true,string=\"hello\" 222",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();

        let columns = batch.columns();

        assert_eq!(batch.row_count(), 3);
        assert_eq!(columns.len(), 7);

        let col = columns.get(0).unwrap();
        assert_eq!(col.name(), "bool");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = col.values().bool_values().unwrap();
        assert_eq!(&values, &[None, None, Some(true)]);

        let col = columns.get(1).unwrap();
        assert_eq!(col.name(), "host");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Tag);
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[Some("a"), Some("a"), None]);

        let col = columns.get(2).unwrap();
        assert_eq!(col.name(), "region");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Tag);
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[None, Some("west"), None]);

        let col = columns.get(3).unwrap();
        assert_eq!(col.name(), "string");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[None, None, Some("hello")]);

        let col = columns.get(4).unwrap();
        assert_eq!(col.name(), TIME_COLUMN_NAME);
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Time);
        let values = col.values().i64_values().unwrap();
        assert_eq!(&values, &[Some(983), Some(2343), Some(222)]);

        let col = columns.get(5).unwrap();
        assert_eq!(col.name(), "val");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = col.values().i64_values().unwrap();
        assert_eq!(&values, &[Some(23), None, Some(21)]);

        let col = columns.get(6).unwrap();
        assert_eq!(col.name(), "val2");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = col.values().f64_values().unwrap();
        assert_eq!(&values, &[None, Some(23.2), None]);
    }

    #[test]
    fn null_mask_builder() {
        let mut m = NullMaskBuilder::new();
        m.push(true);
        m.push(false);
        m.push(true);
        assert_eq!(m.row_count(), 3);
        assert_eq!(m.to_bool_vec(), vec![true, false, true]);
    }

    #[test]
    fn null_mask_builder_eight_edge_case() {
        let mut m = NullMaskBuilder::new();
        m.push(false);
        m.push(true);
        m.push(true);
        m.push(false);
        m.push(false);
        m.push(true);
        m.push(true);
        m.push(false);
        assert_eq!(m.row_count(), 8);
        assert_eq!(
            m.to_bool_vec(),
            vec![false, true, true, false, false, true, true, false]
        );
    }

    #[test]
    fn null_mask_builder_more_than_eight() {
        let mut m = NullMaskBuilder::new();
        m.push(false);
        m.push(true);
        m.push(true);
        m.push(false);
        m.push(false);
        m.push(true);
        m.push(false);
        m.push(false);
        m.push(false);
        m.push(true);
        assert_eq!(m.row_count(), 10);
        assert_eq!(
            m.to_bool_vec(),
            vec![false, true, true, false, false, true, false, false, false, true]
        );
    }

    #[test]
    fn row_count_edge_cases() {
        let lp = vec!["a val=1i 1"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();
        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();
        let columns = batch.columns();

        assert_eq!(batch.row_count(), 1);
        let col = columns.get(1).unwrap();
        assert_eq!(col.name(), "val");
        let values = col.values().i64_values().unwrap();
        assert_eq!(&values, &[Some(1)]);

        let lp = vec![
            "a val=1i 1",
            "a val=1i 2",
            "a val=1i 3",
            "a val=1i 4",
            "a val=1i 5",
            "a val=1i 6",
            "a val2=1i 7",
            "a val=1i 8",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();
        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();
        let columns = batch.columns();

        assert_eq!(batch.row_count(), 8);
        let col = columns.get(1).unwrap();
        assert_eq!(col.name(), "val");
        let values = col.values().i64_values().unwrap();
        assert_eq!(
            &values,
            &[
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                None,
                Some(1)
            ]
        );

        let lp = vec![
            "a val=1i 1",
            "a val=1i 2",
            "a val=1i 3",
            "a val=1i 4",
            "a val=1i 5",
            "a val=1i 6",
            "a val2=1i 7",
            "a val=1i 8",
            "a val=1i 9",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();
        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();
        let columns = batch.columns();

        assert_eq!(batch.row_count(), 9);
        let col = columns.get(1).unwrap();
        assert_eq!(col.name(), "val");
        let values = col.values().i64_values().unwrap();
        assert_eq!(
            &values,
            &[
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                None,
                Some(1),
                Some(1)
            ]
        );
    }

    #[test]
    fn missing_times() {
        let lp = vec!["a val=1i", "a val=2i 123"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let t = Utc::now().timestamp_nanos();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();
        let columns = batch.columns();

        let col = columns.get(0).unwrap();
        assert_eq!(col.name(), TIME_COLUMN_NAME);
        let values = col.values().i64_values().unwrap();
        assert!(values[0].unwrap() > t);
        assert_eq!(values[1], Some(123));
    }

    #[test]
    fn field_type_conflict() {
        let lp = vec!["a val=1i 1", "a val=2.1 123"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1));

        assert!(sharded_entries.is_err());
    }

    #[test]
    fn logical_type_conflict() {
        let lp = vec!["a,host=a val=1i 1", "a host=\"b\" 123"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1));

        assert!(sharded_entries.is_err());
    }

    #[test]
    fn sequenced_entry() {
        let lp = vec![
            "a,host=a val=23i 983",
            "a,host=a,region=west val2=23.2 2343",
            "a val=21i,bool=true,string=\"hello\" 222",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let entry_bytes = sharded_entries.first().unwrap().entry.data();
        let clock_value = ClockValue::new(23);
        let sequenced_entry =
            SequencedEntry::new_from_entry_bytes(clock_value, 2, entry_bytes).unwrap();
        assert_eq!(sequenced_entry.clock_value(), clock_value);
        assert_eq!(sequenced_entry.writer_id(), 2);

        let partition_writes = sequenced_entry.partition_writes().unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();

        let columns = batch.columns();

        assert_eq!(batch.row_count(), 3);
        assert_eq!(columns.len(), 7);

        let col = columns.get(0).unwrap();
        assert_eq!(col.name(), "bool");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = col.values().bool_values().unwrap();
        assert_eq!(&values, &[None, None, Some(true)]);

        let col = columns.get(1).unwrap();
        assert_eq!(col.name(), "host");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Tag);
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[Some("a"), Some("a"), None]);

        let col = columns.get(2).unwrap();
        assert_eq!(col.name(), "region");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Tag);
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[None, Some("west"), None]);

        let col = columns.get(3).unwrap();
        assert_eq!(col.name(), "string");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = match col.values() {
            TypedValuesIterator::String(v) => v,
            _ => panic!("wrong type"),
        };
        let values = values.collect::<Vec<_>>();
        assert_eq!(&values, &[None, None, Some("hello")]);

        let col = columns.get(4).unwrap();
        assert_eq!(col.name(), TIME_COLUMN_NAME);
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Time);
        let values = col.values().i64_values().unwrap();
        assert_eq!(&values, &[Some(983), Some(2343), Some(222)]);

        let col = columns.get(5).unwrap();
        assert_eq!(col.name(), "val");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = col.values().i64_values().unwrap();
        assert_eq!(&values, &[Some(23), None, Some(21)]);

        let col = columns.get(6).unwrap();
        assert_eq!(col.name(), "val2");
        assert_eq!(col.logical_type(), entry_fb::LogicalColumnType::Field);
        let values = col.values().f64_values().unwrap();
        assert_eq!(&values, &[None, Some(23.2), None]);
    }
}
