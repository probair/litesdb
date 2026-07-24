// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

mod observation;
mod schema;
mod sq1;
mod value;

pub use observation::{Observation, ObservationEntry};
pub use schema::{FieldSchema, TableSpec, TableVersion, Validity, VersionSpec};
pub use sq1::Sq1;
pub use value::{CellValue, F32Bits, ValueType};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TableId(u32);

impl TableId {
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SeriesId(u64);

impl SeriesId {
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct FieldId(u16);

impl FieldId {
    #[must_use]
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamKey {
    table: TableId,
    series: SeriesId,
    field: FieldId,
}

impl StreamKey {
    #[must_use]
    pub const fn new(table: TableId, series: SeriesId, field: FieldId) -> Self {
        Self {
            table,
            series,
            field,
        }
    }

    #[must_use]
    pub const fn table(self) -> TableId {
        self.table
    }

    #[must_use]
    pub const fn series(self) -> SeriesId {
        self.series
    }

    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }
}

#[cfg(test)]
#[path = "ids_tests.rs"]
mod tests;
