// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{ops::Range, path::PathBuf};

use litesdb_core::{
    CellValue, CompactLevel, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry,
    SeriesId, StreamKey, TableId, Validity, ValueType, VersionSpec,
};

pub(crate) enum Command {
    Init {
        root: PathBuf,
    },
    CreateTable {
        root: PathBuf,
        spec: VersionSpec,
    },
    Append {
        root: PathBuf,
        table: TableId,
        observation: Observation,
    },
    Sync {
        root: PathBuf,
    },
    Seal {
        root: PathBuf,
    },
    Maintain {
        root: PathBuf,
    },
    Compact {
        root: PathBuf,
        level: CompactLevel,
    },
    Retain {
        root: PathBuf,
        cutoff: i64,
    },
    Scan {
        root: PathBuf,
        key: StreamKey,
        range: Range<i64>,
    },
    ValueAt {
        root: PathBuf,
        keys: Vec<StreamKey>,
        timestamp: i64,
    },
    Latest {
        root: PathBuf,
        keys: Vec<StreamKey>,
    },
    Aggregate {
        root: PathBuf,
        keys: Vec<StreamKey>,
        range: Range<i64>,
        width: u32,
    },
    Sample {
        root: PathBuf,
        keys: Vec<StreamKey>,
        range: Range<i64>,
        step: u32,
    },
}

pub(crate) fn parse<I>(arguments: I) -> Result<Command, String>
where
    I: IntoIterator<Item = String>,
{
    let mut args: Vec<String> = arguments.into_iter().collect();
    if args.len() < 2 {
        return Err(usage());
    }
    let operation = args.remove(0);
    let root = PathBuf::from(args.remove(0));
    match operation.as_str() {
        "init" if args.is_empty() => Ok(Command::Init { root }),
        "create-table" => parse_create(root, &args),
        "append" => parse_append(root, &args),
        "sync" if args.is_empty() => Ok(Command::Sync { root }),
        "seal" if args.is_empty() => Ok(Command::Seal { root }),
        "maintain" if args.is_empty() => Ok(Command::Maintain { root }),
        "compact" if args.len() == 1 => Ok(Command::Compact {
            root,
            level: match args[0].as_str() {
                "l0" => CompactLevel::Level0To1,
                "l1" => CompactLevel::Level1To2,
                _ => return Err("compact level must be l0 or l1".to_owned()),
            },
        }),
        "retain" if args.len() == 1 => Ok(Command::Retain {
            root,
            cutoff: scalar(&args[0], "cutoff")?,
        }),
        "scan" if args.len() == 3 => Ok(Command::Scan {
            root,
            key: stream_key(&args[0])?,
            range: range(&args[1], &args[2])?,
        }),
        "value-at" if args.len() >= 2 => {
            let timestamp = scalar(args.last().ok_or_else(usage)?, "timestamp")?;
            let keys = keys(&args[..args.len().saturating_sub(1)])?;
            Ok(Command::ValueAt {
                root,
                keys,
                timestamp,
            })
        }
        "latest" if !args.is_empty() => Ok(Command::Latest {
            root,
            keys: keys(&args)?,
        }),
        "aggregate" if args.len() >= 4 => {
            let tail = args.len().saturating_sub(3);
            Ok(Command::Aggregate {
                root,
                keys: keys(&args[..tail])?,
                range: range(&args[tail], &args[tail.saturating_add(1)])?,
                width: scalar(&args[tail.saturating_add(2)], "width")?,
            })
        }
        "sample" if args.len() >= 4 => {
            let tail = args.len().saturating_sub(3);
            Ok(Command::Sample {
                root,
                keys: keys(&args[..tail])?,
                range: range(&args[tail], &args[tail.saturating_add(1)])?,
                step: scalar(&args[tail.saturating_add(2)], "step")?,
            })
        }
        _ => Err(usage()),
    }
}

fn parse_create(root: PathBuf, args: &[String]) -> Result<Command, String> {
    if args.len() < 2 {
        return Err(usage());
    }
    let validity = if args[0] == "forever" {
        Validity::Forever
    } else {
        Validity::duration_seconds(scalar(&args[0], "validity")?)
            .map_err(|error| error.to_string())?
    };
    let fields = args[1..]
        .iter()
        .map(|field| {
            let (id, value_type) = pair(field, "field")?;
            Ok(FieldSchema::new(
                FieldId::new(scalar(id, "field id")?),
                match value_type {
                    "uint" => ValueType::UInt,
                    "sq1" => ValueType::Sq1,
                    "f32" => ValueType::F32Bits,
                    _ => return Err("field type must be uint, sq1, or f32".to_owned()),
                },
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let spec = VersionSpec::new(validity, fields).map_err(|error| error.to_string())?;
    Ok(Command::CreateTable { root, spec })
}

fn parse_append(root: PathBuf, args: &[String]) -> Result<Command, String> {
    if args.len() < 3 {
        return Err(usage());
    }
    let table = TableId::new(scalar(&args[0], "table")?);
    let timestamp = scalar(&args[1], "timestamp")?;
    let entries = args[2..]
        .iter()
        .map(|entry| parse_entry(entry))
        .collect::<Result<Vec<_>, _>>()?;
    let observation = Observation::new(timestamp, entries).map_err(|error| error.to_string())?;
    Ok(Command::Append {
        root,
        table,
        observation,
    })
}

fn parse_entry(raw: &str) -> Result<ObservationEntry, String> {
    let mut parts = raw.splitn(3, ':');
    let series = parts
        .next()
        .ok_or_else(|| "entry lacks series".to_owned())?;
    let field = parts.next().ok_or_else(|| "entry lacks field".to_owned())?;
    let value = parts.next().ok_or_else(|| "entry lacks value".to_owned())?;
    Ok(ObservationEntry::new(
        SeriesId::new(scalar(series, "series")?),
        FieldId::new(scalar(field, "field")?),
        parse_value(value)?,
    ))
}

fn parse_value(raw: &str) -> Result<CellValue, String> {
    if raw == "null" {
        return Ok(CellValue::Null);
    }
    let (kind, payload) = pair(raw, "value")?;
    match kind {
        "u" => Ok(CellValue::UInt(scalar(payload, "uint")?)),
        "q" => Ok(CellValue::sq1(scalar(payload, "sq1")?)),
        "f" => u32::from_str_radix(payload.trim_start_matches("0x"), 16)
            .map(F32Bits::from_bits)
            .map(CellValue::F32Bits)
            .map_err(|_| "f32 payload must be hexadecimal bits".to_owned()),
        _ => Err("value must be null, u:N, q:CODE, or f:HEX_BITS".to_owned()),
    }
}

fn keys(raw: &[String]) -> Result<Vec<StreamKey>, String> {
    raw.iter().map(|key| stream_key(key)).collect()
}

fn stream_key(raw: &str) -> Result<StreamKey, String> {
    let mut parts = raw.split(':');
    let table = parts.next().ok_or_else(|| "key lacks table".to_owned())?;
    let series = parts.next().ok_or_else(|| "key lacks series".to_owned())?;
    let field = parts.next().ok_or_else(|| "key lacks field".to_owned())?;
    if parts.next().is_some() {
        return Err("key has too many components".to_owned());
    }
    Ok(StreamKey::new(
        TableId::new(scalar(table, "table")?),
        SeriesId::new(scalar(series, "series")?),
        FieldId::new(scalar(field, "field")?),
    ))
}

fn range(start: &str, end: &str) -> Result<Range<i64>, String> {
    let range = scalar(start, "range start")?..scalar(end, "range end")?;
    if range.start > range.end {
        Err("range start must not exceed end".to_owned())
    } else {
        Ok(range)
    }
}

fn pair<'a>(raw: &'a str, name: &str) -> Result<(&'a str, &'a str), String> {
    raw.split_once(':')
        .filter(|(_, right)| !right.contains(':'))
        .ok_or_else(|| format!("{name} must contain one colon"))
}

fn scalar<T: std::str::FromStr>(raw: &str, name: &str) -> Result<T, String> {
    raw.parse().map_err(|_| format!("invalid {name}: {raw}"))
}

fn usage() -> String {
    "usage: litesdb-cli <init|create-table|append|sync|seal|maintain|compact|retain|scan|value-at|latest|aggregate|sample> ROOT ...".to_owned()
}

#[cfg(test)]
#[path = "args_tests.rs"]
mod tests;
