// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#[path = "resource_probe/fixture.rs"]
mod fixture;
#[path = "resource_probe/multi.rs"]
mod multi;
#[path = "resource_probe/support.rs"]
mod support;
#[path = "resource_probe/year.rs"]
mod year;

use std::{error::Error, path::Path, path::PathBuf, time::Instant};

use litesdb_core::{Db, ErrorKind, OpenOptions, SyncPolicy};

use support::{
    directory_bytes, process_metrics, report, require_absent, single_observation, single_spec,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("mode is required")?;
    let root = PathBuf::from(args.next().ok_or("root is required")?);
    match mode.as_str() {
        "idle" => idle(&root),
        "prepare-multi" => multi::prepare(&root, parse_count(args.next(), "instance count")?),
        "multi" => multi::run(&root, parse_count(args.next(), "instance count")?),
        "prepare-recovery" => prepare_recovery(&root, parse_count(args.next(), "record count")?),
        "recovery" => recovery(&root),
        "seal" => seal(&root, parse_count(args.next(), "record count")?),
        "year" => year::run(&root, parse_days(args.next())?),
        _ => Err(
            "mode must be idle, prepare-multi, multi, prepare-recovery, recovery, seal, or year"
                .into(),
        ),
    }
}

fn parse_days(raw: Option<String>) -> Result<u64, Box<dyn Error>> {
    let value = match raw {
        Some(raw) => raw.parse()?,
        None => 365,
    };
    if value == 0 {
        Err("day count must be positive".into())
    } else {
        Ok(value)
    }
}

fn parse_count(raw: Option<String>, name: &str) -> Result<usize, Box<dyn Error>> {
    let value: usize = raw.ok_or_else(|| format!("{name} is required"))?.parse()?;
    if value == 0 {
        Err(format!("{name} must be positive").into())
    } else {
        Ok(value)
    }
}

fn idle(root: &Path) -> Result<(), Box<dyn Error>> {
    require_absent(root)?;
    let before = process_metrics()?;
    let started = Instant::now();
    let database = Db::open(root, OpenOptions::default())?;
    let elapsed = started.elapsed();
    let after = process_metrics()?;
    report("baseline", &before);
    report("idle", &after);
    println!("open_us\t{}", elapsed.as_micros());
    println!(
        "rss_delta_kib\t{}",
        after.rss_kib.saturating_sub(before.rss_kib)
    );
    drop(database);
    Ok(())
}

fn prepare_recovery(root: &Path, count: usize) -> Result<(), Box<dyn Error>> {
    require_absent(root)?;
    let options = OpenOptions {
        sync_policy: SyncPolicy::Manual,
        ..OpenOptions::default()
    };
    let database = Db::open(root, options)?;
    let table = database.create_table(single_spec())?;
    let mut accepted = 0_usize;
    for index in 0..count {
        let timestamp = i64::try_from(index)?
            .checked_add(1)
            .ok_or("timestamp overflow")?;
        match database.append(table, &single_observation(timestamp, u64::try_from(index)?)) {
            Ok(_) => accepted = accepted.checked_add(1).ok_or("record count overflow")?,
            Err(error) if error.kind() == ErrorKind::ResourceExhausted => break,
            Err(error) => return Err(error.into()),
        }
    }
    database.sync()?;
    println!("accepted_records\t{accepted}");
    println!("disk_bytes\t{}", directory_bytes(root)?);
    Ok(())
}

fn recovery(root: &Path) -> Result<(), Box<dyn Error>> {
    let before = process_metrics()?;
    let started = Instant::now();
    let database = Db::open(root, OpenOptions::default())?;
    let elapsed = started.elapsed();
    let after = process_metrics()?;
    report("baseline", &before);
    report("recovery", &after);
    println!("recovery_ms\t{}", elapsed.as_millis());
    println!(
        "rss_delta_kib\t{}",
        after.rss_kib.saturating_sub(before.rss_kib)
    );
    std::hint::black_box(database.snapshot());
    Ok(())
}

fn seal(root: &Path, count: usize) -> Result<(), Box<dyn Error>> {
    require_absent(root)?;
    let database = Db::open(root, OpenOptions::default())?;
    let table = database.create_table(single_spec())?;
    for index in 0..count {
        let timestamp = i64::try_from(index)?
            .checked_add(1)
            .ok_or("timestamp overflow")?;
        database.append(table, &single_observation(timestamp, u64::try_from(index)?))?;
    }
    let before = process_metrics()?;
    let started = Instant::now();
    let seal = database.seal()?;
    let elapsed = started.elapsed();
    let after = process_metrics()?;
    report("pre_seal", &before);
    report("post_seal", &after);
    println!("seal_ms\t{}", elapsed.as_millis());
    println!("unit_id\t{}", seal.unit_id().unwrap_or_default());
    println!("disk_bytes\t{}", directory_bytes(root)?);
    Ok(())
}
