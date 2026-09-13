// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    Member, Segment, SharedDbId, SharedWalOptions, State,
    format::{self, Pointer, SEGMENT_HEADER},
};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir, publish_atomically},
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
};

#[allow(
    clippy::too_many_lines,
    reason = "single-pass recovery preserves ordered validation and publication state"
)]
pub(super) fn scan(
    directory: &DbDir,
    identity: [u8; 16],
    options: SharedWalOptions,
    mut members: BTreeMap<SharedDbId, Member>,
    mut charged: u64,
) -> Result<State> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory.path(Area::Wal))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| format::invalid("non UTF-8 segment filename"))?;
        paths.push(format::parse_segment(name)?);
        if paths.len() as u64 > options.index_bytes / 128 {
            return Err(Error::limit(
                "shared_wal_segments",
                paths.len() as u64,
                options.index_bytes / 128,
            ));
        }
    }
    paths.sort_unstable();
    let mut segments = BTreeMap::new();
    let mut storage = 0_u64;
    let mut next_lsn = None;
    let mut rotate = false;
    for (index, &number) in paths.iter().enumerate() {
        if index > 0 && paths[index.saturating_sub(1)].checked_add(1) != Some(number) {
            return Err(format::invalid("missing middle segment"));
        }
        let path = directory.file(Area::Wal, &format::segment_name(number));
        let mut file = File::open(&path)?;
        let length = file.metadata()?.len();
        storage = format::add(storage, length)?;
        if storage > options.max_bytes {
            return Err(Error::limit(
                "shared_wal_storage_bytes",
                storage,
                options.max_bytes,
            ));
        }
        let evidence = read_boundary(directory, identity, number, &mut file, length)?;
        let final_segment = index.checked_add(1) == Some(paths.len());
        let mut end = evidence.map_or(length, |value| value.0);
        let mut summary = Segment {
            length,
            members: BTreeMap::new(),
        };
        charged = format::add(charged, 128)?;
        if end == 0 || length < SEGMENT_HEADER as u64 {
            if !final_segment && evidence.is_none() {
                return Err(format::invalid("incomplete middle segment header"));
            }
            let next = next_lsn.unwrap_or(1);
            if evidence.is_some_and(|value| value.0 != 0 || value.1 != next) {
                return Err(format::invalid("empty boundary continuation"));
            }
            if evidence.is_none() {
                publish_boundary(directory, identity, number, &mut file, length, 0, next)?;
            }
            next_lsn = Some(next);
            rotate = final_segment;
            segments.insert(number, summary);
            continue;
        }
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0; SEGMENT_HEADER];
        file.read_exact(&mut header)?;
        let first = format::inspect_segment(&header, identity, number)?;
        if next_lsn.is_some_and(|expected| expected != first) {
            return Err(format::invalid("cross-segment LSN gap"));
        }
        let mut next = first;
        let mut offset = SEGMENT_HEADER as u64;
        while offset < end {
            let remaining = end.saturating_sub(offset);
            let mut prefix = [0; 4];
            let possible = if remaining >= 4 {
                file.read_exact(&mut prefix)?;
                format::frame_len(&prefix)
                    .ok()
                    .filter(|size| *size as u64 <= remaining)
            } else {
                None
            };
            let Some(frame_len) = possible else {
                if !final_segment || evidence.is_some() {
                    return Err(format::invalid("incomplete frozen or middle record"));
                }
                publish_boundary(directory, identity, number, &mut file, length, offset, next)?;
                end = offset;
                rotate = true;
                break;
            };
            let mut bytes = vec![0; frame_len];
            bytes[..4].copy_from_slice(&prefix);
            file.read_exact(&mut bytes[4..])?;
            let crc = u32::from_le_bytes(format::array(&bytes, 4)?);
            if crc != crc32fast::hash(&bytes[8..]) {
                if !final_segment || evidence.is_some() || frame_len as u64 != remaining {
                    return Err(format::invalid("nonterminal frame CRC"));
                }
                publish_boundary(directory, identity, number, &mut file, length, offset, next)?;
                end = offset;
                rotate = true;
                break;
            }
            let frame = format::inspect(&bytes)?;
            if frame.lsn != next {
                return Err(format::invalid("LSN discontinuity"));
            }
            let pointer = Pointer {
                segment: number,
                offset,
            };
            let member = members
                .get_mut(&frame.id)
                .ok_or_else(|| format::invalid("record for unregistered member"))?;
            if member.retired && frame.seq > member.checkpoint {
                return Err(format::invalid(
                    "record exceeds permanent retirement boundary",
                ));
            }
            if !member.initialized && !member.retired {
                return Err(format::invalid("record before initial MANIFEST"));
            }
            if member.latest == Pointer::default() {
                if frame.seq > format::add(member.checkpoint, 1)?
                    || (frame.previous != Pointer::default() && frame.previous >= pointer)
                {
                    return Err(format::invalid("missing member prefix"));
                }
            } else if frame.seq != format::add(member.seq, 1)? || frame.previous != member.latest {
                return Err(format::invalid("member sequence or predecessor mismatch"));
            }
            #[cfg(feature = "archive")]
            if let Some(protection) = &mut member.archive {
                protection.replay(frame.seq, frame.raw)?;
            }
            member.latest = pointer;
            member.seq = frame.seq;
            member.lsn = frame.lsn;
            if frame.seq > member.checkpoint {
                member.bytes = format::add(member.bytes, frame_len as u64)?;
            }
            if !summary.members.contains_key(&frame.id) {
                charged = format::add(charged, 96)?;
            }
            summary.members.insert(frame.id, frame.seq);
            if charged > options.index_bytes {
                return Err(Error::limit(
                    "shared_wal_index_bytes",
                    charged,
                    options.index_bytes,
                ));
            }
            offset = format::add(offset, frame_len as u64)?;
            next = format::add(next, 1)?;
        }
        if offset != end || evidence.is_some_and(|value| value.1 != next) {
            return Err(format::invalid("boundary does not match valid prefix"));
        }
        next_lsn = Some(next);
        rotate |= final_segment && evidence.is_some();
        segments.insert(number, summary);
    }
    for member in members.values_mut() {
        if member.seq < member.checkpoint {
            member.seq = member.checkpoint;
            member.latest = Pointer::default();
        }
        member.durable_seq = member.seq;
        member.durable_pointer = member.latest;
        #[cfg(feature = "archive")]
        if let Some(protection) = &mut member.archive {
            if protection.latest.seq != member.seq {
                return Err(format::invalid(
                    "archive anchor missing from recoverable data",
                ));
            }
            protection.durable = protection.latest;
        }
    }
    let next = next_lsn.unwrap_or(1);
    let mut number = paths.last().copied().unwrap_or(0);
    let file = if number == 0 || rotate {
        number = format::add(number, 1)?;
        if format::add(storage, SEGMENT_HEADER as u64)? > options.max_bytes {
            return Err(Error::limit(
                "shared_wal_storage_bytes",
                format::add(storage, SEGMENT_HEADER as u64)?,
                options.max_bytes,
            ));
        }
        let file = super::store::create_segment(directory, identity, number, next)?;
        segments.insert(
            number,
            Segment {
                length: SEGMENT_HEADER as u64,
                members: BTreeMap::new(),
            },
        );
        storage = format::add(storage, SEGMENT_HEADER as u64)?;
        charged = format::add(charged, 128)?;
        file
    } else {
        OpenOptions::new()
            .read(true)
            .append(true)
            .open(directory.file(Area::Wal, &format::segment_name(number)))?
    };
    if charged > options.index_bytes {
        return Err(Error::limit(
            "shared_wal_index_bytes",
            charged,
            options.index_bytes,
        ));
    }
    let offset = file.metadata()?.len();
    file.sync_data()?;
    Ok(State {
        file,
        segment: number,
        offset,
        lsn: next.saturating_sub(1),
        durable: next.saturating_sub(1),
        buffer: Vec::new(),
        storage,
        charged,
        scratch_bytes: 0,
        paths: members
            .iter()
            .filter(|(_, member)| !member.retired)
            .map(|(id, member)| (member.root.clone(), *id))
            .collect(),
        members,
        segments,
        poison: false,
        writes: 0,
        syncs: 0,
        #[cfg(test)]
        fail_write: false,
        #[cfg(test)]
        fail_sync: false,
        #[cfg(test)]
        registration_fault: None,
    })
}
pub(super) fn boundary_name(segment: u64) -> String {
    format!("BOUNDARY-{segment:020}")
}
fn original_crc(file: &mut File) -> Result<u32> {
    let position = file.stream_position()?;
    file.seek(SeekFrom::Start(0))?;
    let mut hasher = crc32fast::Hasher::new();
    let mut bytes = vec![0; 65_536];
    loop {
        let length = file.read(&mut bytes)?;
        if length == 0 {
            break;
        }
        hasher.update(&bytes[..length]);
    }
    file.seek(SeekFrom::Start(position))?;
    Ok(hasher.finalize())
}
fn publish_boundary(
    directory: &DbDir,
    owner: [u8; 16],
    segment: u64,
    file: &mut File,
    length: u64,
    end: u64,
    next: u64,
) -> Result<()> {
    file.sync_all()?;
    let mut bytes = b"LSSB\x02\0\0\0".to_vec();
    bytes.extend_from_slice(&owner);
    bytes.extend_from_slice(&segment.to_le_bytes());
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&original_crc(file)?.to_le_bytes());
    bytes.extend_from_slice(&end.to_le_bytes());
    bytes.extend_from_slice(&next.to_le_bytes());
    publish_atomically(
        directory,
        Area::Root,
        &boundary_name(segment),
        &format::checksum(bytes),
    )
}
fn read_boundary(
    directory: &DbDir,
    owner: [u8; 16],
    segment: u64,
    file: &mut File,
    length: u64,
) -> Result<Option<(u64, u64)>> {
    let path = directory.file(Area::Root, &boundary_name(segment));
    if !path.try_exists()? {
        return Ok(None);
    }
    if fs::metadata(&path)?.len() != 64 {
        return Err(format::invalid("boundary size"));
    }
    let bytes = fs::read(path)?;
    let body = format::checked(&bytes, b"LSSB\x02\0\0\0")?;
    if format::array::<16>(body, 8)? != owner
        || format::u64_at(body, 24)? != segment
        || format::u64_at(body, 32)? != length
        || u32::from_le_bytes(format::array(body, 40)?) != original_crc(file)?
    {
        return Err(format::invalid("boundary original mismatch"));
    }
    let end = format::u64_at(body, 44)?;
    let next = format::u64_at(body, 52)?;
    if end > length || (end != 0 && end < SEGMENT_HEADER as u64) || next == 0 {
        return Err(format::invalid("invalid accepted boundary"));
    }
    Ok(Some((end, next)))
}
