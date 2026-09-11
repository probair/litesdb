// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::Result;

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
}
impl<'a> Reader<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let value = self
            .bytes
            .get(..length)
            .ok_or_else(|| super::invalid("truncated archive field"))?;
        self.bytes = &self.bytes[length..];
        Ok(value)
    }
    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| super::invalid("invalid fixed field"))
    }
    pub(crate) fn magic(&mut self, expected: &[u8]) -> Result<()> {
        if self.take(expected.len())? != expected {
            return Err(super::invalid("unsupported archive format"));
        }
        Ok(())
    }
    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub(crate) const fn remaining(&self) -> &'a [u8] {
        self.bytes
    }
    pub(crate) fn finish(self) -> Result<()> {
        if !self.bytes.is_empty() {
            return Err(super::invalid("trailing archive bytes"));
        }
        Ok(())
    }
}
pub(crate) fn append_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}
pub(crate) fn read_record<'a>(reader: &mut Reader<'a>) -> Result<(u64, u64, u64, &'a [u8])> {
    let epoch = reader.u64()?;
    let segment = reader.u64()?;
    let offset = reader.u64()?;
    let length = reader.u32()?;
    if !(16..=65_552).contains(&length) {
        return Err(super::invalid("invalid archived record length"));
    }
    let raw = reader.take(length as usize)?;
    Ok((epoch, segment, offset, raw))
}
pub(crate) fn encode_record(epoch: u64, segment: u64, offset: u64, raw: &[u8]) -> Result<Vec<u8>> {
    let length =
        u32::try_from(raw.len()).map_err(|_| super::invalid("oversized archived record"))?;
    let mut bytes = Vec::with_capacity(raw.len().saturating_add(28));
    for value in [epoch, segment, offset] {
        append_u64(&mut bytes, value);
    }
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(raw);
    Ok(bytes)
}
