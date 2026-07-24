// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::num::NonZeroU32;

use super::{FieldId, ValueType};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Validity {
    DurationSeconds(NonZeroU32),
    Forever,
}

impl Validity {
    pub fn duration_seconds(seconds: u32) -> Result<Self> {
        NonZeroU32::new(seconds)
            .map(Self::DurationSeconds)
            .ok_or_else(|| Error::invalid("validity", "duration must be positive"))
    }

    #[must_use]
    pub const fn duration(self) -> Option<NonZeroU32> {
        match self {
            Self::DurationSeconds(seconds) => Some(seconds),
            Self::Forever => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FieldSchema {
    field: FieldId,
    value_type: ValueType,
}

impl FieldSchema {
    #[must_use]
    pub const fn new(field: FieldId, value_type: ValueType) -> Self {
        Self { field, value_type }
    }

    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }

    #[must_use]
    pub const fn value_type(self) -> ValueType {
        self.value_type
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionSpec {
    validity: Validity,
    fields: Box<[FieldSchema]>,
}

pub type TableSpec = VersionSpec;

impl VersionSpec {
    pub fn new(validity: Validity, fields: Vec<FieldSchema>) -> Result<Self> {
        if fields
            .windows(2)
            .any(|pair| pair[0].field() >= pair[1].field())
        {
            return Err(Error::invalid(
                "fields",
                "field identifiers must be strictly increasing",
            ));
        }
        Ok(Self {
            validity,
            fields: fields.into_boxed_slice(),
        })
    }

    #[must_use]
    pub const fn validity(&self) -> Validity {
        self.validity
    }

    #[must_use]
    pub fn fields(&self) -> &[FieldSchema] {
        &self.fields
    }

    #[allow(
        dead_code,
        reason = "used by Db::new_table_version in a later milestone"
    )]
    pub(crate) fn validate_successor(&self, previous: &TableVersion) -> Result<()> {
        for old in previous.fields() {
            let index = self
                .fields
                .binary_search_by_key(&old.field(), |field| field.field())
                .map_err(|_| Error::invalid("fields", "existing field cannot be removed"))?;
            if self.fields[index].value_type() != old.value_type() {
                return Err(Error::invalid(
                    "fields",
                    "existing field cannot change value type",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableVersion {
    version_no: u32,
    validity: Validity,
    fields: Box<[FieldSchema]>,
    effective_from: Option<i64>,
}

impl TableVersion {
    #[allow(
        dead_code,
        reason = "consumed by MANIFEST decoding in the next M3 module"
    )]
    pub(crate) fn restore(
        version_no: u32,
        validity: Validity,
        fields: Vec<FieldSchema>,
        effective_from: Option<i64>,
    ) -> Result<Self> {
        if version_no == 0 {
            return Err(Error::corruption(
                "table version",
                "version number starts at one",
            ));
        }
        let spec = VersionSpec::new(validity, fields)
            .map_err(|_| Error::corruption("table version", "fields are not strictly ordered"))?;
        Ok(Self {
            version_no,
            validity: spec.validity,
            fields: spec.fields,
            effective_from,
        })
    }

    #[allow(
        dead_code,
        reason = "used by table catalog construction in a later milestone"
    )]
    pub(crate) fn initial(spec: VersionSpec) -> Self {
        Self {
            version_no: 1,
            validity: spec.validity,
            fields: spec.fields,
            effective_from: None,
        }
    }

    #[allow(
        dead_code,
        reason = "used by Db::new_table_version in a later milestone"
    )]
    pub(crate) fn successor(&self, spec: VersionSpec) -> Result<Self> {
        spec.validate_successor(self)?;
        let version_no = self
            .version_no
            .checked_add(1)
            .ok_or_else(|| Error::limit("table_version_no", 4_294_967_296, u64::from(u32::MAX)))?;
        Ok(Self {
            version_no,
            validity: spec.validity,
            fields: spec.fields,
            effective_from: None,
        })
    }

    #[allow(
        dead_code,
        reason = "used when the first observation activates a version"
    )]
    pub(crate) fn activate(mut self, timestamp: i64) -> Result<Self> {
        if self.effective_from.is_some() {
            return Err(Error::invalid("table_version", "version is already active"));
        }
        self.effective_from = Some(timestamp);
        Ok(self)
    }

    #[must_use]
    pub const fn version_no(&self) -> u32 {
        self.version_no
    }

    #[must_use]
    pub const fn validity(&self) -> Validity {
        self.validity
    }

    #[must_use]
    pub fn fields(&self) -> &[FieldSchema] {
        &self.fields
    }

    #[must_use]
    pub const fn effective_from(&self) -> Option<i64> {
        self.effective_from
    }
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
