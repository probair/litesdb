// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{Error, Result, TableVersion, limits::MAX_LOGICAL_STREAMS};

pub(super) fn validate_versions(last_ts: Option<i64>, versions: &[TableVersion]) -> Result<()> {
    if versions.is_empty()
        || versions.len() > usize::try_from(MAX_LOGICAL_STREAMS).unwrap_or(usize::MAX)
    {
        return Err(Error::corruption(
            "MANIFEST table",
            "version count is invalid",
        ));
    }
    let mut previous_effective = None;
    for (index, version) in versions.iter().enumerate() {
        let expected = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| Error::corruption("MANIFEST table", "version number overflow"))?;
        if version.version_no() != expected {
            return Err(Error::corruption(
                "MANIFEST table",
                "version numbers are not continuous",
            ));
        }
        if let Some(previous) = versions.get(index.saturating_sub(1)).filter(|_| index != 0) {
            validate_field_compatibility(previous, version)?;
        }
        match version.effective_from() {
            Some(effective) if previous_effective.is_some_and(|prior| effective <= prior) => {
                return Err(Error::corruption(
                    "MANIFEST table",
                    "version activation times are not increasing",
                ));
            }
            Some(effective) => previous_effective = Some(effective),
            None if index.checked_add(1) != Some(versions.len()) => {
                return Err(Error::corruption(
                    "MANIFEST table",
                    "only the latest version may be inactive",
                ));
            }
            None => {}
        }
    }
    if previous_effective.is_some() != last_ts.is_some()
        || previous_effective
            .zip(last_ts)
            .is_some_and(|(effective, last)| last < effective)
    {
        return Err(Error::corruption(
            "MANIFEST table",
            "table clock contradicts version activation",
        ));
    }
    Ok(())
}

fn validate_field_compatibility(previous: &TableVersion, current: &TableVersion) -> Result<()> {
    for field in previous.fields() {
        let index = current
            .fields()
            .binary_search_by_key(&field.field(), |candidate| candidate.field())
            .map_err(|_| Error::corruption("MANIFEST table", "version removed a field"))?;
        if current.fields()[index].value_type() != field.value_type() {
            return Err(Error::corruption(
                "MANIFEST table",
                "version changed a field type",
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_count(name: &'static str, actual: usize, maximum: u32) -> Result<()> {
    let actual =
        u64::try_from(actual).map_err(|_| Error::limit(name, u64::MAX, u64::from(maximum)))?;
    if actual > u64::from(maximum) {
        return Err(Error::limit(name, actual, u64::from(maximum)));
    }
    Ok(())
}
