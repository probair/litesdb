// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by query primitives later in M5")]

use crate::{Error, FieldId, Result, TableVersion, Validity, ValueType};

#[derive(Clone, Copy, Debug)]
pub(crate) struct VersionContract<'a> {
    version: &'a TableVersion,
    value_type: ValueType,
    effective_start: i64,
    effective_end: Option<i64>,
}

impl<'a> VersionContract<'a> {
    pub(crate) fn resolve(
        versions: &'a [TableVersion],
        field: FieldId,
        fact_ts: i64,
    ) -> Result<Self> {
        let active = versions.partition_point(|version| {
            version
                .effective_from()
                .is_some_and(|effective| effective <= fact_ts)
        });
        let index = active
            .checked_sub(1)
            .ok_or_else(|| Error::corruption("version contract", "Fact predates every version"))?;
        let version = versions
            .get(index)
            .ok_or_else(|| Error::corruption("version contract", "active version is absent"))?;
        let effective_start = version.effective_from().ok_or_else(|| {
            Error::corruption("version contract", "Fact resolved to an inactive version")
        })?;
        let effective_end = versions
            .get(index.saturating_add(1))
            .and_then(TableVersion::effective_from);
        let field_index = version
            .fields()
            .binary_search_by_key(&field, |schema| schema.field())
            .map_err(|_| {
                Error::corruption(
                    "version contract",
                    "field is absent from historical version",
                )
            })?;
        Ok(Self {
            version,
            value_type: version.fields()[field_index].value_type(),
            effective_start,
            effective_end,
        })
    }

    pub(crate) const fn version(self) -> &'a TableVersion {
        self.version
    }

    pub(crate) const fn interpretation(self) -> ValueType {
        self.value_type
    }

    pub(crate) const fn validity(self) -> Validity {
        self.version.validity()
    }

    pub(crate) const fn effective_range(self) -> (i64, Option<i64>) {
        (self.effective_start, self.effective_end)
    }

    pub(crate) fn is_live(self, fact_ts: i64, query_ts: i64) -> bool {
        if query_ts < fact_ts {
            return false;
        }
        match self.validity() {
            Validity::Forever => true,
            Validity::DurationSeconds(seconds) => i128::from(query_ts)
                .checked_sub(i128::from(fact_ts))
                .is_some_and(|elapsed| elapsed < i128::from(seconds.get())),
        }
    }
}

#[cfg(test)]
#[path = "contract_tests.rs"]
mod tests;
