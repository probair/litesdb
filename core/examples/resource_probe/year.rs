// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{error::Error, fs, path::Path, time::Instant};

use litesdb_core::{
    CellValue, CompactLevel, Db, FieldId, FieldSchema, Observation, ObservationEntry, OpenOptions,
    SeriesId, SyncPolicy, Validity, ValueType, VersionSpec,
};

use super::{
    fixture::{
        CADENCE_SECONDS, DAY_SECONDS, DIGEST_OFFSET, HostFixture, HostSample, STREAM_COUNT,
        update_dense_digest,
    },
    support::{directory_bytes, process_metrics, report, require_absent},
};

const TABLE_COUNT: u64 = 4;

pub(super) fn run(root: &Path, days: u64) -> Result<(), Box<dyn Error>> {
    require_absent(root)?;
    let started = Instant::now();
    let database = Db::open(
        root,
        OpenOptions {
            sync_policy: SyncPolicy::Manual,
            ..OpenOptions::default()
        },
    )?;
    let high = database.create_table(spec(
        Validity::duration_seconds(9)?,
        &[
            ValueType::Sq1,
            ValueType::UInt,
            ValueType::UInt,
            ValueType::UInt,
            ValueType::UInt,
            ValueType::UInt,
            ValueType::UInt,
        ],
    ))?;
    let ping = database.create_table(spec(Validity::duration_seconds(45)?, &[ValueType::UInt]))?;
    let disk = database.create_table(spec(Validity::duration_seconds(180)?, &[ValueType::UInt]))?;
    let event = database.create_table(spec(
        Validity::Forever,
        &[ValueType::UInt, ValueType::UInt, ValueType::UInt],
    ))?;
    let samples_per_day = DAY_SECONDS
        .checked_div(CADENCE_SECONDS)
        .ok_or("bad cadence")?;
    let quarter = samples_per_day.checked_div(4).ok_or("bad day partition")?;
    let mut fixture = HostFixture::new();
    let mut facts = 0_u64;
    let mut text_bytes = 0_u64;
    let mut high_samples = 0_u64;
    let mut ping_samples = 0_u64;
    let mut ping_nulls = [0_u64; 3];
    let mut restart_events = 0_u64;
    let mut dense_digest = DIGEST_OFFSET;

    for _day in 0..days {
        for within_day in 0..samples_per_day {
            let sample = fixture.next()?;
            dense_digest = update_dense_digest(dense_digest, &sample);
            append_counted(
                &database,
                high,
                "high",
                high_observation(&sample),
                &mut facts,
                &mut text_bytes,
            )?;
            high_samples = high_samples.checked_add(1).ok_or("sample count overflow")?;

            if sample.ping_due {
                append_counted(
                    &database,
                    ping,
                    "ping",
                    ping_observation(&sample),
                    &mut facts,
                    &mut text_bytes,
                )?;
                ping_samples = ping_samples.checked_add(1).ok_or("ping count overflow")?;
                for (count, value) in ping_nulls.iter_mut().zip(sample.ping_ms) {
                    if value.is_none() {
                        *count = count.checked_add(1).ok_or("ping null count overflow")?;
                    }
                }
            }
            if sample.disk_due {
                append_counted(
                    &database,
                    disk,
                    "disk",
                    disk_observation(&sample),
                    &mut facts,
                    &mut text_bytes,
                )?;
            }
            if sample.initial || sample.restart {
                append_counted(
                    &database,
                    event,
                    "event",
                    event_observation(&sample),
                    &mut facts,
                    &mut text_bytes,
                )?;
                if sample.restart {
                    restart_events = restart_events
                        .checked_add(1)
                        .ok_or("restart count overflow")?;
                }
            }
            maintain(&database, within_day, quarter, samples_per_day)?;
        }
    }
    database.sync()?;
    let disk_bytes = directory_bytes(root)?;
    let metrics = process_metrics()?;
    report("year", &metrics);
    println!("tables\t{TABLE_COUNT}");
    println!("streams\t{STREAM_COUNT}");
    println!("days\t{days}");
    println!("high_frequency_samples\t{high_samples}");
    println!("facts\t{facts}");
    println!("restart_events\t{restart_events}");
    println!("ping_samples_per_route\t{ping_samples}");
    println!("ping_tokyo_nulls\t{}", ping_nulls[0]);
    println!("ping_us_central_nulls\t{}", ping_nulls[1]);
    println!("ping_frankfurt_nulls\t{}", ping_nulls[2]);
    println!(
        "ping_loss_pct_x100\t{}/{}/{}",
        loss_pct_x100(ping_nulls[0], ping_samples)?,
        loss_pct_x100(ping_nulls[1], ping_samples)?,
        loss_pct_x100(ping_nulls[2], ping_samples)?,
    );
    println!("dense_digest\t{dense_digest:016x}");
    println!("elapsed_ms\t{}", started.elapsed().as_millis());
    println!("text_bytes\t{text_bytes}");
    println!("disk_bytes\t{disk_bytes}");
    println!(
        "compression_x100\t{}",
        text_bytes.checked_mul(100).ok_or("ratio overflow")? / disk_bytes.max(1)
    );
    println!("unit_files\t{}", fs::read_dir(root.join("units"))?.count());
    Ok(())
}

fn maintain(
    database: &Db,
    within_day: u64,
    quarter: u64,
    samples_per_day: u64,
) -> Result<(), Box<dyn Error>> {
    let completed = within_day.checked_add(1).ok_or("day position overflow")?;
    if completed % quarter == 0 {
        database.seal()?;
        if completed % quarter.checked_mul(2).ok_or("partition overflow")? == 0 {
            database.compact(CompactLevel::Level0To1)?;
        }
        if completed == samples_per_day {
            database.compact(CompactLevel::Level1To2)?;
        }
    }
    Ok(())
}

fn append_counted(
    database: &Db,
    table: litesdb_core::TableId,
    table_name: &str,
    observation: Observation,
    facts: &mut u64,
    text_bytes: &mut u64,
) -> Result<(), Box<dyn Error>> {
    let fact_count = u64::try_from(observation.entries().len())?;
    *text_bytes = text_bytes
        .checked_add(observation_text_bytes(table_name, &observation)?)
        .ok_or("text byte count overflow")?;
    *facts = facts.checked_add(fact_count).ok_or("Fact count overflow")?;
    database.append(table, &observation)?;
    Ok(())
}

fn spec(validity: Validity, types: &[ValueType]) -> VersionSpec {
    let fields = types
        .iter()
        .enumerate()
        .map(|(index, value_type)| {
            FieldSchema::new(
                FieldId::new(u16::try_from(index.saturating_add(1)).unwrap_or(u16::MAX)),
                *value_type,
            )
        })
        .collect();
    VersionSpec::new(validity, fields).unwrap_or_else(|_| unreachable!("static schema is valid"))
}

fn high_observation(sample: &HostSample) -> Observation {
    observation(
        sample.timestamp,
        vec![
            entry(1, 1, CellValue::sq1(sample.cpu_sq1)),
            entry(1, 2, CellValue::UInt(sample.load_x100[0])),
            entry(1, 3, CellValue::UInt(sample.load_x100[1])),
            entry(1, 4, CellValue::UInt(sample.load_x100[2])),
            entry(1, 5, CellValue::UInt(sample.mem_usage_bytes)),
            entry(1, 6, CellValue::UInt(sample.net_rx_bytes)),
            entry(1, 7, CellValue::UInt(sample.net_tx_bytes)),
        ],
    )
}

fn ping_observation(sample: &HostSample) -> Observation {
    observation(
        sample.timestamp,
        sample
            .ping_ms
            .iter()
            .enumerate()
            .map(|(index, value)| {
                entry(
                    u64::try_from(index.saturating_add(1)).unwrap_or(u64::MAX),
                    1,
                    value.map_or(CellValue::Null, CellValue::UInt),
                )
            })
            .collect(),
    )
}

fn disk_observation(sample: &HostSample) -> Observation {
    observation(
        sample.timestamp,
        vec![entry(1, 1, CellValue::UInt(sample.disk_usage_bytes))],
    )
}

fn event_observation(sample: &HostSample) -> Observation {
    let entries = if sample.initial {
        vec![
            entry(1, 1, CellValue::UInt(sample.mem_capacity_bytes)),
            entry(1, 2, CellValue::UInt(sample.disk_capacity_bytes)),
            entry(1, 3, CellValue::UInt(sample.boot_timestamp)),
        ]
    } else {
        vec![entry(1, 3, CellValue::UInt(sample.boot_timestamp))]
    };
    observation(sample.timestamp, entries)
}

fn observation(timestamp: i64, entries: Vec<ObservationEntry>) -> Observation {
    Observation::new(timestamp, entries)
        .unwrap_or_else(|_| unreachable!("generated observation ordering is valid"))
}

fn entry(series: u64, field: u16, value: CellValue) -> ObservationEntry {
    ObservationEntry::new(SeriesId::new(series), FieldId::new(field), value)
}

fn observation_text_bytes(table: &str, observation: &Observation) -> Result<u64, Box<dyn Error>> {
    let mut total = 0_u64;
    for entry in observation.entries() {
        let payload_len = value_len(entry.value())?;
        let line = decimal_len(observation.timestamp().unsigned_abs())
            .checked_add(u64::try_from(table.len())?)
            .and_then(|value| value.checked_add(decimal_len(entry.series().get())))
            .and_then(|value| value.checked_add(decimal_len(u64::from(entry.field().get()))))
            .and_then(|value| value.checked_add(payload_len))
            .and_then(|value| value.checked_add(5))
            .ok_or("text line length overflow")?;
        total = total.checked_add(line).ok_or("text byte count overflow")?;
    }
    Ok(total)
}

fn value_len(value: CellValue) -> Result<u64, Box<dyn Error>> {
    Ok(match value {
        CellValue::Null => 4,
        CellValue::UInt(value) => decimal_len(value),
        CellValue::Sq1(value) => decimal_len(u64::from(value.code())),
        CellValue::F32Bits(_) => 10,
        _ => return Err("unsupported textual value type".into()),
    })
}

fn decimal_len(mut value: u64) -> u64 {
    let mut digits = 1_u64;
    while value >= 10 {
        value /= 10;
        digits = digits.saturating_add(1);
    }
    digits
}

fn loss_pct_x100(nulls: u64, samples: u64) -> Result<u64, Box<dyn Error>> {
    Ok(nulls
        .checked_mul(10_000)
        .ok_or("ping loss ratio overflow")?
        / samples.max(1))
}
