// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{error::Error, fs, os::unix::fs::symlink, path::Path, time::Instant};

use litesdb_core::{
    CellValue, Db, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry, OpenOptions,
    SeriesId, SyncPolicy, Validity, ValueType, VersionSpec,
};

use super::support::{process_metrics, report, require_absent, require_present};

const CATALOG_TABLES: usize = 4;
const CATALOG_UNITS: usize = 400;
const FIXTURE_NAME: &str = "_fixture";

pub(super) fn prepare(root: &Path, count: usize) -> Result<(), Box<dyn Error>> {
    require_absent(root)?;
    fs::create_dir_all(root)?;
    let fixture = root.join(FIXTURE_NAME);
    build_fixture(&fixture)?;
    for index in 0..count {
        clone_shell(&fixture, &instance_path(root, index))?;
    }
    println!("instances\t{count}");
    println!("catalog_tables\t{CATALOG_TABLES}");
    println!("catalog_units\t{CATALOG_UNITS}");
    println!("shared_immutable_fixture\ttrue");
    Ok(())
}

pub(super) fn run(root: &Path, count: usize) -> Result<(), Box<dyn Error>> {
    require_present(root)?;
    let before = process_metrics()?;
    let started = Instant::now();
    let mut databases = Vec::with_capacity(count);
    for index in 0..count {
        databases.push(Db::open(
            &instance_path(root, index),
            OpenOptions {
                sync_policy: SyncPolicy::Manual,
                ..OpenOptions::default()
            },
        )?);
    }
    let elapsed = started.elapsed();
    let after = process_metrics()?;
    let delta = after.rss_kib.saturating_sub(before.rss_kib);
    report("baseline", &before);
    report("multi", &after);
    println!("instances\t{count}");
    println!("catalog_tables\t{CATALOG_TABLES}");
    println!("catalog_units\t{CATALOG_UNITS}");
    println!("open_ms\t{}", elapsed.as_millis());
    println!("rss_delta_kib\t{delta}");
    println!(
        "rss_bytes_per_db\t{}",
        delta
            .saturating_mul(1024)
            .checked_div(u64::try_from(count)?)
            .unwrap_or_default()
    );
    std::hint::black_box(&databases);
    Ok(())
}

fn build_fixture(root: &Path) -> Result<(), Box<dyn Error>> {
    let database = Db::open(
        root,
        OpenOptions {
            sync_policy: SyncPolicy::Manual,
            ..OpenOptions::default()
        },
    )?;
    let mut tables = Vec::with_capacity(CATALOG_TABLES);
    for _ in 0..CATALOG_TABLES {
        tables.push(database.create_table(catalog_spec())?);
    }
    for index in 0..CATALOG_UNITS {
        let timestamp = i64::try_from(index)?
            .checked_add(1)
            .ok_or("timestamp overflow")?;
        for table in &tables {
            database.append(
                *table,
                &catalog_observation(timestamp, u64::try_from(index)?),
            )?;
        }
        database.seal()?;
    }
    database.sync()?;
    Ok(())
}

fn instance_path(root: &Path, index: usize) -> std::path::PathBuf {
    root.join(format!("db-{index:05}"))
}

fn clone_shell(fixture: &Path, target: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(target)?;
    fs::copy(fixture.join("MANIFEST"), target.join("MANIFEST"))?;
    for child in ["wal", "heads", "agg", "tmp"] {
        fs::create_dir_all(target.join(child))?;
    }
    copy_regular_files(&fixture.join("wal"), &target.join("wal"))?;
    let units = fixture.join("units").canonicalize()?;
    symlink(units, target.join("units"))?;
    Ok(())
}

fn copy_regular_files(source: &Path, target: &Path) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), target.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn catalog_spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::Sq1),
            FieldSchema::new(FieldId::new(3), ValueType::F32Bits),
        ],
    )
    .unwrap_or_else(|_| unreachable!("static schema is valid"))
}

fn catalog_observation(timestamp: i64, value: u64) -> Observation {
    let low = u8::try_from(value % 200).unwrap_or_default();
    let float_low = u32::try_from(value % 65_536).unwrap_or_default();
    Observation::new(
        timestamp,
        vec![
            ObservationEntry::new(SeriesId::new(1), FieldId::new(1), CellValue::UInt(value)),
            ObservationEntry::new(SeriesId::new(1), FieldId::new(2), CellValue::sq1(low)),
            ObservationEntry::new(
                SeriesId::new(1),
                FieldId::new(3),
                CellValue::F32Bits(F32Bits::from_bits(0x3f80_0000 | float_low)),
            ),
        ],
    )
    .unwrap_or_else(|_| unreachable!("static observation ordering is valid"))
}
