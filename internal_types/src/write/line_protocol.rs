use std::borrow::Cow;

use hashbrown::HashMap;
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use snafu::{ResultExt, Snafu};

use crate::schema::TIME_COLUMN_NAME;
use crate::write::builder::ColumnWriteBuilder;
use crate::write::TableWrite;
use chrono::Utc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Parse error at line {}: {}", line_number, source))]
    ParseError {
        line_number: usize,
        source: influxdb_line_protocol::Error,
    },

    #[snafu(display("Column error at line {}: {}", line_number, source))]
    ColumnError {
        line_number: usize,
        source: crate::write::builder::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Options {
    default_time: i64,
    tag_dictionary: bool,
    tag_packed: bool,
    string_dictionary: bool,
    string_packed: bool,
    bool_packed: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            default_time: Utc::now().timestamp_nanos(),
            tag_dictionary: false,
            tag_packed: false,
            string_dictionary: false,
            string_packed: false,
            bool_packed: false,
        }
    }
}

pub fn lp_to_table_writes<'a>(
    lp: &'a str,
    options: &Options,
) -> Result<HashMap<Cow<'a, str>, TableWrite<'a>>> {
    lines_to_table_writes(parse_lines(lp), options)
}

pub fn lines_to_table_writes<'a>(
    lines: impl IntoIterator<Item = Result<ParsedLine<'a>, influxdb_line_protocol::Error>>,
    options: &Options,
) -> Result<HashMap<Cow<'a, str>, TableWrite<'a>>> {
    let mut tables: HashMap<Cow<'a, str>, (usize, HashMap<Cow<'a, str>, ColumnWriteBuilder<'a>>)> =
        Default::default();

    for (idx, line) in lines.into_iter().enumerate() {
        let line = line.context(ParseError {
            line_number: idx + 1,
        })?;

        let (rows, table) = tables.entry(line.series.measurement.into()).or_default();
        *rows += 1;

        if let Some(tagset) = line.series.tag_set {
            for (key, value) in tagset {
                let builder = table.entry(key.into()).or_insert_with(|| {
                    ColumnWriteBuilder::new_tag_column(options.tag_dictionary, options.tag_packed)
                });
                builder.push_tag(value.into()).context(ColumnError {
                    line_number: idx + 1,
                })?
            }
        }

        for (key, value) in line.field_set {
            match value {
                FieldValue::I64(data) => {
                    let builder = table
                        .entry(key.into())
                        .or_insert_with(|| ColumnWriteBuilder::new_i64_column());
                    builder.push_i64(data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::U64(data) => {
                    let builder = table
                        .entry(key.into())
                        .or_insert_with(|| ColumnWriteBuilder::new_u64_column());
                    builder.push_u64(data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::F64(data) => {
                    let builder = table
                        .entry(key.into())
                        .or_insert_with(|| ColumnWriteBuilder::new_f64_column());
                    builder.push_f64(data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::String(data) => {
                    let builder = table.entry(key.into()).or_insert_with(|| {
                        ColumnWriteBuilder::new_string_column(
                            options.string_dictionary,
                            options.string_packed,
                        )
                    });
                    builder.push_string(data.into()).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::Boolean(data) => {
                    let builder = table.entry(key.into()).or_insert_with(|| {
                        ColumnWriteBuilder::new_bool_column(options.bool_packed)
                    });
                    builder.push_bool(data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
            }
        }

        let builder = table
            .entry(TIME_COLUMN_NAME.into())
            .or_insert_with(|| ColumnWriteBuilder::new_time_column());

        builder
            .push_time(line.timestamp.unwrap_or_else(|| options.default_time))
            .unwrap();

        for builder in table.values_mut() {
            builder.null_to_idx(*rows)
        }
    }

    Ok(tables
        .into_iter()
        .map(|(name, (_, columns))| {
            (
                name,
                TableWrite {
                    columns: columns
                        .into_iter()
                        .map(|(column_name, builder)| (column_name, builder.build()))
                        .collect(),
                },
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{InfluxColumnType, InfluxFieldType};

    #[test]
    fn test_basic() {
        let lp = r#"
            a,host=a ival=23i,fval=1.2,uval=7u,sval="hi",bval=true 1
            a,host=b ival=22i,fval=2.2,uval=1u,sval="world",bval=false 2
        "#;

        let writes = lp_to_table_writes(lp, &Options::default()).unwrap();

        assert_eq!(writes.len(), 1);
        assert_eq!(writes["a"].columns.len(), 7);

        let columns = &writes["a"].columns;

        assert_eq!(columns["host"].influx_type, InfluxColumnType::Tag);
        assert_eq!(columns["host"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["host"].values.string().unwrap(), &["a", "b"]);

        assert_eq!(
            columns["ival"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Integer)
        );
        assert_eq!(columns["ival"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["ival"].values.i64().unwrap(), &[23, 22]);

        assert_eq!(
            columns["fval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Float)
        );
        assert_eq!(columns["fval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["fval"].values.f64().unwrap(), &[1.2, 2.2]);

        assert_eq!(
            columns["uval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::UInteger)
        );
        assert_eq!(columns["uval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["uval"].values.u64().unwrap(), &[7, 1]);

        assert_eq!(
            columns["sval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(columns["sval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["sval"].values.string().unwrap(), &["hi", "world"]);

        assert_eq!(
            columns["bval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Boolean)
        );
        assert_eq!(columns["bval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["bval"].values.bool().unwrap(), &[true, false]);

        assert_eq!(
            columns[TIME_COLUMN_NAME].influx_type,
            InfluxColumnType::Timestamp
        );
        assert_eq!(columns[TIME_COLUMN_NAME].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns[TIME_COLUMN_NAME].values.i64().unwrap(), &[1, 2]);
    }

    #[test]
    fn test_nulls() {
        let lp = r#"
            a,host=a val=23i 983
            a val=21i,bool=true,string="hello" 222
            a,host=a,region=west val2=23.2
        "#;

        let options = Options {
            default_time: 32,
            ..Default::default()
        };

        let writes = lp_to_table_writes(lp, &options).unwrap();

        assert_eq!(writes.len(), 1);
        assert_eq!(writes["a"].columns.len(), 7);

        let columns = &writes["a"].columns;

        assert_eq!(
            columns[TIME_COLUMN_NAME].influx_type,
            InfluxColumnType::Timestamp
        );
        assert_eq!(columns[TIME_COLUMN_NAME].valid_mask.as_ref(), &[0b00000111]);
        assert_eq!(
            columns[TIME_COLUMN_NAME].values.i64().unwrap(),
            &[983, 222, options.default_time]
        );

        assert_eq!(columns["host"].influx_type, InfluxColumnType::Tag);
        assert_eq!(columns["host"].valid_mask.as_ref(), &[0b00000101]);
        assert_eq!(columns["host"].values.string().unwrap(), &["a", "a"]);

        assert_eq!(
            columns["val"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Integer)
        );
        assert_eq!(columns["val"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["val"].values.i64().unwrap(), &[23, 21]);
    }

    #[test]
    fn test_multiple_table() {
        let lp = r#"
            cpu val=1 55
            mem val=23 10
            cpu val=88 100
            disk foo=23.2 110
            mem val=55 111
        "#;
        let writes = lp_to_table_writes(lp, &Options::default()).unwrap();
        assert_eq!(writes.len(), 3);
        assert!(writes.contains_key("cpu"));
        assert!(writes.contains_key("mem"));
        assert!(writes.contains_key("disk"));
    }

    #[test]
    fn test_packed_strings() {
        let lp = r#"
            a,foo=bar val="cupcakes" 1
            a,foo=bar val="bongo" 2
            a,foo=banana val="cupcakes" 3
            a,foo=bar val="cupcakes" 4
            a,foo=bar val="bongo" 5
        "#;
        let options = Options {
            string_packed: true,
            tag_dictionary: true,
            ..Default::default()
        };

        let writes = lp_to_table_writes(lp, &options).unwrap();
        assert_eq!(writes.len(), 1);
        let columns = &writes["a"].columns;

        assert_eq!(
            columns["val"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        let val = columns["val"].values.packed_string().unwrap();
        assert_eq!(val.values.as_ref(), "cupcakesbongocupcakescupcakesbongo");
        assert_eq!(val.indexes.as_ref(), &[0, 8, 13, 21, 29, 34]);

        assert_eq!(columns["foo"].influx_type, InfluxColumnType::Tag);
        let foo = columns["foo"].values.dictionary().unwrap();
        assert_eq!(foo.keys.as_ref(), &[0, 0, 1, 0, 0]);
        assert_eq!(foo.values.values.as_ref(), "barbanana");
        assert_eq!(foo.values.indexes.as_ref(), &[0, 3, 9]);
    }

    #[test]
    fn test_packed_bool() {
        let lp = r#"
            a,foo=bar val=true 1
            a,foo=bar val=true 2
            a,foo=banana val=false 3
            a,foo=bar val=true 4
            a,foo=bar val=false 5
        "#;
        let options = Options {
            tag_packed: true,
            bool_packed: true,
            ..Default::default()
        };

        let writes = lp_to_table_writes(lp, &options).unwrap();
        assert_eq!(writes.len(), 1);
        let columns = &writes["a"].columns;

        assert_eq!(
            columns["val"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Boolean)
        );
        assert_eq!(columns["val"].values.packed_bool().unwrap(), &[0b00001011]);

        assert_eq!(columns["foo"].influx_type, InfluxColumnType::Tag);
        let foo = columns["foo"].values.packed_string().unwrap();
        assert_eq!(foo.values.as_ref(), "barbarbananabarbar");
        assert_eq!(foo.indexes.as_ref(), &[0, 3, 6, 12, 15, 18]);
    }
}
