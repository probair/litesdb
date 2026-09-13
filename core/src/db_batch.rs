// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Db, Seq, caller_error};
#[cfg(feature = "bench-metrics")]
use crate::bench_metrics::{self, Counter, Span, Stage};
use crate::{
    Error, Observation, Result, TableId,
    wal::{RecordBody, ReplayTarget},
};
use std::sync::Arc;

impl Db {
    pub fn try_append_batch(
        &self,
        table: TableId,
        observations: &[Observation],
    ) -> Result<Option<Seq>> {
        if observations.is_empty() {
            return Err(Error::invalid("observations", "batch must be nonempty"));
        }
        #[cfg(feature = "bench-metrics")]
        let lock_profile = Span::new(Stage::MutationLock);
        let mut engine = match self.engine.try_lock() {
            Ok(engine) => engine,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(Error::Poisoned),
        };
        #[cfg(feature = "bench-metrics")]
        drop(lock_profile);
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::Append);
        #[cfg(feature = "bench-metrics")]
        {
            bench_metrics::count(Counter::ObservationAttempts, observations.len() as u64);
            bench_metrics::count(
                Counter::ObservationEntries,
                observations.iter().fold(0_u64, |total, observation| {
                    total.saturating_add(observation.entries().len() as u64)
                }),
            );
        }
        engine.writer.ensure_healthy()?;
        #[cfg(feature = "bench-metrics")]
        let validate_profile = Span::new(Stage::MutationValidate);
        let first = engine.tail.next_seq();
        let mut growth = 0_u64;
        let mut previous = None;
        for observation in observations {
            if previous.is_some_and(|timestamp| timestamp >= observation.timestamp()) {
                return Err(Error::invalid(
                    "timestamp",
                    "batch timestamps must strictly increase",
                ));
            }
            previous = Some(observation.timestamp());
            engine
                .tail
                .validate(
                    first,
                    &RecordBody::AppendObservation {
                        table,
                        observation: observation.clone(),
                    },
                )
                .map_err(caller_error)?;
            growth = growth
                .checked_add(Self::estimated_append_growth(observation.entries().len())?)
                .ok_or_else(|| {
                    Error::limit("tail_index_bytes", u64::MAX, Self::tail_limit_bytes())
                })?;
        }
        let total = engine
            .tail
            .estimated_bytes()
            .checked_add(growth)
            .ok_or_else(|| Error::limit("tail_index_bytes", u64::MAX, Self::tail_limit_bytes()))?;
        if total > Self::tail_limit_bytes() {
            return Err(Error::limit(
                "tail_index_bytes",
                total,
                Self::tail_limit_bytes(),
            ));
        }
        #[cfg(feature = "bench-metrics")]
        drop(validate_profile);
        let last = engine.writer.append_batch(table, observations)?;
        #[cfg(feature = "bench-metrics")]
        let _tail_profile = Span::new(Stage::TailApply);
        #[cfg(feature = "bench-metrics")]
        bench_metrics::count(Counter::TailBytesBeforeApply, engine.tail.estimated_bytes());
        self.invalidate_visible();
        let mut seq = first;
        for observation in observations {
            if Arc::make_mut(&mut engine.tail)
                .apply(
                    seq,
                    RecordBody::AppendObservation {
                        table,
                        observation: observation.clone(),
                    },
                )
                .is_err()
            {
                engine.writer.mark_poisoned();
                return Err(Error::corruption(
                    "Db batch",
                    "prevalidated batch tail publication failed",
                ));
            }
            seq = seq
                .checked_add(1)
                .ok_or_else(|| Error::corruption("Db batch", "sequence overflow"))?;
        }
        engine.maintenance_due |= total >= u64::from(self.options.seal_policy.memory_bytes);
        #[cfg(feature = "bench-metrics")]
        bench_metrics::count(Counter::AppliedRecords, observations.len() as u64);
        Ok(Some(Seq(last)))
    }
}
