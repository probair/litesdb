// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::pedantic,
    clippy::unwrap_used,
    clippy::expect_used
)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod args;
mod output;

use std::process::ExitCode;

use args::Command;
use litesdb_core::{CompactReport, Db, OpenOptions, SealReport};

fn main() -> ExitCode {
    match args::parse(std::env::args().skip(1)).and_then(run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run(command: Command) -> Result<(), String> {
    let root = command_root(&command);
    let database = Db::open(root, OpenOptions::default()).map_err(|error| error.to_string())?;
    dispatch(&database, command)
}

fn command_root(command: &Command) -> &std::path::Path {
    match command {
        Command::Init { root }
        | Command::CreateTable { root, .. }
        | Command::Append { root, .. }
        | Command::Sync { root }
        | Command::Seal { root }
        | Command::Maintain { root }
        | Command::Compact { root, .. }
        | Command::Retain { root, .. }
        | Command::Scan { root, .. }
        | Command::ValueAt { root, .. }
        | Command::Latest { root, .. }
        | Command::Aggregate { root, .. }
        | Command::Sample { root, .. } => root,
    }
}

fn dispatch(database: &Db, command: Command) -> Result<(), String> {
    match command {
        Command::Init { .. } => println!("ok"),
        Command::CreateTable { spec, .. } => {
            let table = database
                .create_table(spec)
                .map_err(|error| error.to_string())?;
            println!("{}", table.get());
        }
        Command::Append {
            table, observation, ..
        } => {
            let sequence = database
                .append(table, &observation)
                .map_err(|error| error.to_string())?;
            println!("{}", sequence.get());
        }
        Command::Sync { .. } => {
            let durable = database.sync().map_err(|error| error.to_string())?;
            println!(
                "{}\t{}\t{}",
                durable.seq(),
                durable.segment(),
                durable.offset()
            );
        }
        Command::Seal { .. } => {
            let report = database.seal().map_err(|error| error.to_string())?;
            println!(
                "{}\t{}",
                report
                    .unit_id()
                    .map_or_else(|| "none".to_owned(), |id| id.to_string()),
                report.checkpointed_records()
            );
        }
        Command::Maintain { .. } => {
            let report = database.maintain().map_err(|error| error.to_string())?;
            println!(
                "{}\t{}\t{}\t{}\t{}",
                report
                    .synced()
                    .map_or_else(|| "none".to_owned(), |position| position.seq().to_string()),
                report
                    .sealed()
                    .and_then(SealReport::unit_id)
                    .map_or_else(|| "none".to_owned(), |id| id.to_string()),
                report
                    .compacted_l1()
                    .and_then(CompactReport::output_unit)
                    .map_or_else(|| "none".to_owned(), |id| id.to_string()),
                report
                    .compacted_l2()
                    .and_then(CompactReport::output_unit)
                    .map_or_else(|| "none".to_owned(), |id| id.to_string()),
                report.more_due()
            );
        }
        Command::Compact { level, .. } => {
            let report = database.compact(level).map_err(|error| error.to_string())?;
            println!(
                "{}\t{}",
                report.input_units(),
                report
                    .output_unit()
                    .map_or_else(|| "none".to_owned(), |id| id.to_string())
            );
        }
        Command::Retain { cutoff, .. } => {
            let report = database.retain(cutoff).map_err(|error| error.to_string())?;
            println!(
                "{}\t{}",
                report.removed_units(),
                report
                    .floor()
                    .map_or_else(|| "none".to_owned(), |floor| floor.to_string())
            );
        }
        query @ (Command::Scan { .. }
        | Command::ValueAt { .. }
        | Command::Latest { .. }
        | Command::Aggregate { .. }
        | Command::Sample { .. }) => return run_query(database, query),
    }
    Ok(())
}

fn run_query(database: &Db, command: Command) -> Result<(), String> {
    match command {
        Command::Scan { key, range, .. } => {
            let snapshot = database.snapshot();
            let mut cursor = snapshot
                .scan(key, range)
                .map_err(|error| error.to_string())?;
            while let Some(fact) = cursor.next_fact().map_err(|error| error.to_string())? {
                println!("{}\t{}", fact.timestamp(), output::cell(fact.value()));
            }
        }
        Command::ValueAt {
            keys, timestamp, ..
        } => {
            for value in database
                .snapshot()
                .value_at(&keys, timestamp)
                .map_err(|error| error.to_string())?
            {
                println!("{}", output::lookup(value));
            }
        }
        Command::Latest { keys, .. } => {
            for value in database
                .snapshot()
                .latest(&keys)
                .map_err(|error| error.to_string())?
            {
                println!("{}", output::lookup(value));
            }
        }
        Command::Aggregate {
            keys, range, width, ..
        } => {
            for (key_index, row) in database
                .snapshot()
                .aggregate(&keys, range, width)
                .map_err(|error| error.to_string())?
                .into_iter()
                .enumerate()
            {
                for value in row {
                    println!("{key_index}\t{}", output::bucket(value));
                }
            }
        }
        Command::Sample {
            keys, range, step, ..
        } => {
            for (key_index, row) in database
                .snapshot()
                .sample(&keys, range, step)
                .map_err(|error| error.to_string())?
                .into_iter()
                .enumerate()
            {
                for (slot_index, value) in row.into_iter().enumerate() {
                    println!("{key_index}\t{slot_index}\t{}", output::slot(value));
                }
            }
        }
        Command::Init { .. }
        | Command::CreateTable { .. }
        | Command::Append { .. }
        | Command::Sync { .. }
        | Command::Seal { .. }
        | Command::Maintain { .. }
        | Command::Compact { .. }
        | Command::Retain { .. } => {
            return Err("internal command dispatch mismatch".to_owned());
        }
    }
    Ok(())
}
