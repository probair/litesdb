// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::Db;
use crate::{OpenOptions, Result, wal::ReplayTarget};
impl Db {
    pub(crate) fn open_for_restore(root: &std::path::Path, options: OpenOptions) -> Result<Self> {
        Self::open_configured(root, options, None, true).map(|(db, _)| db)
    }
    pub(crate) fn validate_restored_storage(&self) -> Result<()> {
        self.lock_engine()?.source.validate_all()
    }
    pub(crate) fn replay_archived_record(&self, raw: &[u8]) -> Result<()> {
        let mut engine = self.lock_engine()?;
        let record = crate::wal::record::decode(raw, engine.tail.next_seq(), |table, field| {
            engine.tail.field_type(table, field)
        })?;
        self.mutate_locked(&mut engine, record.body())?;
        Ok(())
    }
}
