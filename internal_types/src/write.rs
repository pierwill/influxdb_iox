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

impl<'a> ColumnWriteValues<'a> {
    pub fn f64(&self) -> Option<&[f64]> {
        match &self {
            Self::F64(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    pub fn i64(&self) -> Option<&[i64]> {
        match &self {
            Self::I64(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    pub fn u64(&self) -> Option<&[u64]> {
        match &self {
            Self::U64(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    pub fn string(&self) -> Option<&[Cow<'a, str>]> {
        match &self {
            Self::String(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    pub fn packed_string(&self) -> Option<&PackedStrings<'a>> {
        match &self {
            Self::PackedString(data) => Some(data),
            _ => None,
        }
    }

    pub fn dictionary(&self) -> Option<&Dictionary<'a>> {
        match &self {
            Self::Dictionary(data) => Some(data),
            _ => None,
        }
    }

    pub fn packed_bool(&self) -> Option<&[u8]> {
        match &self {
            Self::PackedBool(data) => Some(data.as_ref()),
            _ => None,
        }
    }

    pub fn bool(&self) -> Option<&[bool]> {
        match &self {
            Self::Bool(data) => Some(data.as_ref()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PackedStrings<'a> {
    pub indexes: Cow<'a, [u16]>,
    pub values: Cow<'a, str>,
}

#[derive(Debug, Clone)]
pub struct Dictionary<'a> {
    pub keys: Cow<'a, [u16]>,
    pub values: PackedStrings<'a>,
}
