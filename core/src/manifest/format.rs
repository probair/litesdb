// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by MANIFEST store later in M3")]

use crc32fast::hash;

use crate::{
    Error, Result, TableVersion, Validity,
    limits::MAX_MANIFEST_BODY_BYTES,
    manifest::catalog::{Manifest, TableCatalog},
};

#[path = "format_decode.rs"]
mod decode_impl;

pub(crate) const MANIFEST_HEADER_BYTES: usize = 24;
const MANIFEST_MAGIC: [u8; 4] = *b"LSM1";
const MANIFEST_FORMAT_VERSION: u16 = 1;

pub(crate) fn encode(manifest: &Manifest) -> Result<Vec<u8>> {
    let measured = measure_body(manifest)?;
    let maximum = usize::try_from(MAX_MANIFEST_BODY_BYTES).map_err(|_| {
        Error::limit(
            "manifest_body_bytes",
            u64::MAX,
            u64::from(MAX_MANIFEST_BODY_BYTES),
        )
    })?;
    if measured > maximum {
        return Err(Error::limit(
            "manifest_body_bytes",
            u64::try_from(measured).unwrap_or(u64::MAX),
            u64::from(MAX_MANIFEST_BODY_BYTES),
        ));
    }
    let mut body = Vec::with_capacity(measured);
    encode_body(manifest, &mut body)?;
    if body.len() != measured {
        return Err(Error::corruption(
            "MANIFEST",
            "encoded body differs from measurement",
        ));
    }
    let body_len = u32::try_from(body.len()).map_err(|_| {
        Error::limit(
            "manifest_body_bytes",
            u64::MAX,
            u64::from(MAX_MANIFEST_BODY_BYTES),
        )
    })?;
    if body_len > MAX_MANIFEST_BODY_BYTES {
        return Err(Error::limit(
            "manifest_body_bytes",
            u64::from(body_len),
            u64::from(MAX_MANIFEST_BODY_BYTES),
        ));
    }
    let file_len = body
        .len()
        .checked_add(MANIFEST_HEADER_BYTES)
        .ok_or_else(|| Error::limit("manifest_file_bytes", u64::MAX, u64::from(u32::MAX)))?;
    let mut bytes = Vec::with_capacity(file_len);
    bytes.extend_from_slice(&MANIFEST_MAGIC);
    put_u16(&mut bytes, MANIFEST_FORMAT_VERSION);
    put_u16(&mut bytes, 0);
    put_u64(&mut bytes, manifest.identity().generation());
    put_u32(&mut bytes, body_len);
    put_u32(&mut bytes, hash(&body));
    bytes.extend_from_slice(&body);
    if bytes.len() != file_len {
        return Err(Error::corruption(
            "MANIFEST",
            "encoded file differs from measured length",
        ));
    }
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, Default)]
struct BodySize(usize);

impl BodySize {
    fn add(&mut self, bytes: usize) -> Result<()> {
        self.0 = self.0.checked_add(bytes).ok_or_else(|| {
            Error::limit(
                "manifest_body_bytes",
                u64::MAX,
                u64::from(MAX_MANIFEST_BODY_BYTES),
            )
        })?;
        Ok(())
    }

    fn add_items(&mut self, count: usize, width: usize) -> Result<()> {
        let bytes = count.checked_mul(width).ok_or_else(|| {
            Error::limit(
                "manifest_body_bytes",
                u64::MAX,
                u64::from(MAX_MANIFEST_BODY_BYTES),
            )
        })?;
        self.add(bytes)
    }
}

fn measure_body(manifest: &Manifest) -> Result<usize> {
    let mut size = BodySize::default();
    size.add(24 + 36)?;
    size.add(option_width(manifest.retention().floor().is_some()))?;
    size.add(option_width(
        manifest.retention().heads_generation().is_some(),
    ))?;
    size.add(4)?;
    for table in manifest.tables() {
        size.add(4)?;
        size.add(option_width(table.last_ts().is_some()))?;
        size.add(4)?;
        for version in table.versions() {
            size.add(4)?;
            size.add(if version.validity().duration().is_some() {
                5
            } else {
                1
            })?;
            size.add(option_width(version.effective_from().is_some()))?;
            size.add(4)?;
            size.add_items(version.fields().len(), 3)?;
        }
        size.add(4)?;
        size.add_items(table.retired_series().len(), 16)?;
        size.add(4)?;
        size.add_items(table.retired_fields().len(), 10)?;
    }
    size.add(4)?;
    size.add_items(manifest.units().len(), 49)?;
    Ok(size.0)
}

const fn option_width(is_some: bool) -> usize {
    if is_some { 9 } else { 1 }
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Manifest> {
    decode_impl::decode(bytes)
}

fn encode_body(manifest: &Manifest, output: &mut Vec<u8>) -> Result<()> {
    let checkpoint = manifest.checkpoint();
    put_u64(output, checkpoint.segment_first_seq());
    put_u64(output, checkpoint.offset());
    put_u64(output, checkpoint.next_seq());

    let identity = manifest.identity();
    put_u64(output, identity.unit_high_water());
    put_u32(output, identity.table_high_water());
    put_u64(output, identity.generation());
    put_u64(output, identity.shard_id());
    put_u64(output, identity.writer_epoch());

    let retention = manifest.retention();
    put_option_i64(output, retention.floor());
    put_option_u64(output, retention.heads_generation());

    put_count(output, manifest.tables().len(), "tables")?;
    for table in manifest.tables() {
        encode_table(table, output)?;
    }
    put_count(output, manifest.units().len(), "live_units")?;
    for unit in manifest.units() {
        put_u64(output, unit.unit_id());
        output.push(unit.level());
        put_i64(output, unit.min_ts());
        put_i64(output, unit.max_ts());
        put_u32(output, unit.section_count());
        put_u64(output, unit.total_rows());
        put_u64(output, unit.file_len());
        put_u32(output, unit.body_crc32());
    }
    Ok(())
}

fn encode_table(table: &TableCatalog, output: &mut Vec<u8>) -> Result<()> {
    put_u32(output, table.table().get());
    put_option_i64(output, table.last_ts());
    put_count(output, table.versions().len(), "table_versions")?;
    for version in table.versions() {
        encode_version(version, output)?;
    }
    put_count(output, table.retired_series().len(), "retired_series")?;
    for retirement in table.retired_series() {
        put_u64(output, retirement.series().get());
        put_i64(output, retirement.retire_ts());
    }
    put_count(output, table.retired_fields().len(), "retired_fields")?;
    for retirement in table.retired_fields() {
        put_u16(output, retirement.field().get());
        put_i64(output, retirement.retire_ts());
    }
    Ok(())
}

fn encode_version(version: &TableVersion, output: &mut Vec<u8>) -> Result<()> {
    put_u32(output, version.version_no());
    match version.validity() {
        Validity::DurationSeconds(seconds) => {
            output.push(0);
            put_u32(output, seconds.get());
        }
        Validity::Forever => output.push(1),
    }
    put_option_i64(output, version.effective_from());
    put_count(output, version.fields().len(), "fields")?;
    for field in version.fields() {
        put_u16(output, field.field().get());
        output.push(field.value_type().tag());
    }
    Ok(())
}

fn put_count(output: &mut Vec<u8>, count: usize, name: &'static str) -> Result<()> {
    let count =
        u32::try_from(count).map_err(|_| Error::limit(name, u64::MAX, u64::from(u32::MAX)))?;
    put_u32(output, count);
    Ok(())
}

fn put_option_i64(output: &mut Vec<u8>, value: Option<i64>) {
    match value {
        None => output.push(0),
        Some(value) => {
            output.push(1);
            put_i64(output, value);
        }
    }
}

fn put_option_u64(output: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => output.push(0),
        Some(value) => {
            output.push(1);
            put_u64(output, value);
        }
    }
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
