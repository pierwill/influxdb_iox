//! A generic representation of columnar data that is agnostic to the underlying representation

use crate::schema::InfluxColumnType;
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct TableWrite<'a> {
    pub table_name: Cow<'a, str>,
    pub columns: Cow<'a, [ColumnWrite<'a>]>,
}

#[derive(Debug, Clone)]
pub struct ColumnWrite<'a> {
    pub name: Cow<'a, str>,
    pub row_count: usize,
    pub influx_type: InfluxColumnType,
    pub valid_mask: Cow<'a, [u8]>,
    pub values: ColumnWriteValues<'a>,
}

#[derive(Debug, Clone)]
pub enum ColumnWriteValues<'a> {
    F64(Cow<'a, [f64]>),
    I64(Cow<'a, [i64]>),
    U64(Cow<'a, [u64]>),
    String(Cow<'a, [Cow<'a, str>]>),
    PackedString(PackedStrings<'a>),
    Dictionary(Dictionary<'a>),
    PackedBool(Cow<'a, [u8]>),
    Bool(Cow<'a, [bool]>),
}

#[derive(Debug, Clone)]
pub struct PackedStrings<'a> {
    indexes: Cow<'a, [u16]>,
    values: Cow<'a, str>,
}

#[derive(Debug, Clone)]
pub struct Dictionary<'a> {
    keys: Cow<'a, [u16]>,
    values: PackedStrings<'a>,
}
