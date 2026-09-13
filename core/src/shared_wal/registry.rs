// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    Member, SharedDbId, SharedWal, SharedWalOptions,
    format::{self, Pointer},
};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir, publish_atomically},
    manifest,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
const MAX_REGISTRY_BYTES: u64 = 8192;
pub(super) fn name(id: SharedDbId) -> String {
    use std::fmt::Write as _;
    let mut value = String::from("MEMBER-");
    for byte in id.bytes() {
        let _ = write!(value, "{byte:02x}");
    }
    value
}
fn read(path: &Path) -> Result<Vec<u8>> {
    if fs::metadata(path)?.len() > MAX_REGISTRY_BYTES {
        return Err(format::invalid("oversized member authority"));
    }
    Ok(fs::read(path)?)
}
fn binding(owner: [u8; 16], id: SharedDbId) -> Vec<u8> {
    let mut bytes = b"LSSD\x02\0\0\0".to_vec();
    bytes.extend_from_slice(&owner);
    bytes.extend_from_slice(&id.bytes());
    format::checksum(bytes)
}
pub(crate) fn verify_binding(root: &Path, owner: [u8; 16], id: SharedDbId) -> Result<bool> {
    let path = root.join("SHARED");
    if !path.try_exists()? {
        return Ok(false);
    }
    let bytes = read(&path)?;
    format::checked(&bytes, b"LSSD\x02\0\0\0")?;
    if bytes != binding(owner, id) {
        return Err(format::invalid("foreign logical database binding"));
    }
    Ok(true)
}
pub(crate) fn read_binding(root: &Path) -> Result<Option<([u8; 16], SharedDbId)>> {
    let path = root.join("SHARED");
    if !path.try_exists()? {
        return Ok(None);
    }
    let bytes = read(&path)?;
    let body = format::checked(&bytes, b"LSSD\x02\0\0\0")?;
    if body.len() != 56 {
        return Err(format::invalid("binding size"));
    }
    Ok(Some((
        format::array(body, 8)?,
        SharedDbId::from_bytes(&body[24..56])?,
    )))
}
#[allow(
    clippy::too_many_lines,
    reason = "ordered registry validation covers live, pending-import and retired authority before GC"
)]
pub(super) fn load(
    directory: &DbDir,
    identity: [u8; 16],
    options: SharedWalOptions,
) -> Result<(BTreeMap<SharedDbId, Member>, u64)> {
    let mut members = BTreeMap::new();
    let mut paths = BTreeSet::new();
    let mut charged = 0_u64;
    for entry in fs::read_dir(directory.path(Area::Root))? {
        let entry = entry?;
        let filename = entry.file_name();
        let Some(filename) = filename.to_str() else {
            continue;
        };
        if !filename.starts_with("MEMBER-") {
            continue;
        }
        let bytes = read(&entry.path())?;
        let body = format::checked(&bytes, b"LSSR\x02\0\0\0")?;
        let id = SharedDbId::from_bytes(
            body.get(8..40)
                .ok_or_else(|| format::invalid("registration identity"))?,
        )?;
        let length = u32::from_le_bytes(format::array(body, 40)?) as usize;
        let path_bytes = body
            .get(44..)
            .ok_or_else(|| format::invalid("registration path"))?;
        if path_bytes.len() != length || filename != name(id) {
            return Err(format::invalid("registration framing"));
        }
        let encoded_path = std::str::from_utf8(path_bytes)
            .map_err(|_| format::invalid("registration path encoding"))?;
        let root = if encoded_path == ".." {
            directory
                .path(Area::Root)
                .parent()
                .ok_or_else(|| format::invalid("embedded owner has no parent"))?
                .canonicalize()?
        } else {
            std::path::PathBuf::from(encoded_path)
        };
        if let Some(seq) = super::retire::load(directory, identity, id)? {
            charged = format::add(charged, super::member_charge(&root))?;
            if charged > options.index_bytes || members.len() >= options.max_databases as usize {
                return Err(Error::limit(
                    "shared_wal_index_bytes",
                    charged,
                    options.index_bytes,
                ));
            }
            members.insert(
                id,
                Member {
                    root,
                    initialized: false,
                    retired: true,
                    checkpoint: seq,
                    latest: Pointer::default(),
                    seq,
                    lsn: 0,
                    bytes: 0,
                    durable_seq: seq,
                    durable_pointer: Pointer::default(),
                    #[cfg(feature = "archive")]
                    archive: None,
                },
            );
            continue;
        }
        if !root.is_absolute() || !root.try_exists()? {
            return Err(format::invalid("registered directory missing"));
        }
        let bound = verify_binding(&root, identity, id)?;
        #[cfg(not(feature = "archive"))]
        if root.join("ARCHIVE").try_exists()? {
            return Err(Error::unsupported(
                "open",
                "owner contains archive-protected members",
            ));
        }
        #[cfg(feature = "archive")]
        let archive = super::archive::Protection::load(&root, id)?.map(Box::new);
        let initialized = root.join("MANIFEST").try_exists()?;
        let checkpoint = if initialized {
            if !bound {
                #[cfg(feature = "archive")]
                if root.join("SEALED").try_exists()? {
                    let descriptor = crate::archive::sealed::verify(&root)?;
                    if descriptor.cursor.database != id.database()
                        || descriptor.cursor.generation != id.generation()
                    {
                        return Err(format::invalid("pending sealed identity mismatch"));
                    }
                } else {
                    return Err(format::invalid("MANIFEST without shared binding"));
                }
                #[cfg(not(feature = "archive"))]
                return Err(format::invalid("MANIFEST without shared binding"));
            }
            manifest::load(&DbDir::existing(&root))?
                .checkpoint()
                .next_seq()
                .saturating_sub(1)
        } else {
            0
        };
        charged = format::add(charged, super::member_charge(&root))?;
        #[cfg(feature = "archive")]
        if archive.is_some() {
            charged = format::add(charged, super::archive::PROTECTION_CHARGE)?;
        }
        if charged > options.index_bytes || members.len() >= options.max_databases as usize {
            return Err(Error::limit(
                "shared_wal_index_bytes",
                charged,
                options.index_bytes,
            ));
        }
        if !paths.insert(root.clone()) {
            return Err(format::invalid("duplicate registered path"));
        }
        members.insert(
            id,
            Member {
                root,
                initialized,
                retired: false,
                checkpoint,
                latest: Pointer::default(),
                seq: checkpoint,
                lsn: 0,
                bytes: 0,
                durable_seq: checkpoint,
                durable_pointer: Pointer::default(),
                #[cfg(feature = "archive")]
                archive,
            },
        );
    }
    Ok((members, charged))
}
impl SharedWal {
    pub(crate) fn validate_open(&self, root: &Path, id: SharedDbId) -> Result<()> {
        let state = self.lock()?;
        if let Some(member) = state.members.get(&id) {
            if member.retired {
                return Err(Error::invalid(
                    "database identity",
                    "retired identity cannot be reused",
                ));
            }
            if root.canonicalize()? != member.root {
                return Err(Error::invalid(
                    "database path",
                    "identity belongs to another registered path",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn registered_id(&self, root: &Path) -> Result<Option<SharedDbId>> {
        let root = root.canonicalize()?;
        Ok(self.lock()?.paths.get(&root).copied())
    }

    pub(crate) fn initialized(&self, id: SharedDbId, checkpoint: u64) -> Result<()> {
        let mut state = self.lock()?;
        let member = state
            .members
            .get_mut(&id)
            .ok_or_else(|| format::invalid("missing initialized member"))?;
        if !member.initialized {
            if member.seq != 0 || member.latest != Pointer::default() {
                return Err(format::invalid("records precede initialized checkpoint"));
            }
            member.checkpoint = checkpoint;
            member.seq = checkpoint;
            member.durable_seq = checkpoint;
            member.initialized = true;
        } else if member.checkpoint != checkpoint {
            return Err(format::invalid(
                "registered checkpoint differs from MANIFEST",
            ));
        }
        Ok(())
    }

    pub(crate) fn register(&self, directory: &DbDir, id: SharedDbId) -> Result<()> {
        self.register_inner(directory, id, false)
    }
    pub(crate) fn register_imported(&self, directory: &DbDir, id: SharedDbId) -> Result<()> {
        self.register_inner(directory, id, true)
    }
    #[allow(
        clippy::too_many_lines,
        reason = "registration and binding share one explicit poison-on-publication scope"
    )]
    fn register_inner(&self, directory: &DbDir, id: SharedDbId, imported: bool) -> Result<()> {
        let root = directory.path(Area::Root).canonicalize()?;
        let bound = verify_binding(&root, self.inner.identity, id)?;
        if root.join("MANIFEST").try_exists()? && !bound && !imported {
            return Err(Error::unsupported(
                "shared open",
                "old or unbound MANIFEST is not supported",
            ));
        }
        let mut state = self.lock()?;
        #[cfg(test)]
        if let Some((target, shared, step)) = state.registration_fault.take()
            && target == id
        {
            if shared {
                directory.fail_publish(Area::Root, "SHARED", step);
            } else {
                self.inner
                    .directory
                    .fail_publish(Area::Root, &name(id), step);
            }
        }
        if let Some(member) = state.members.get(&id) {
            if member.retired {
                return Err(Error::invalid(
                    "database identity",
                    "retired identity cannot be reused",
                ));
            }
            if member.root != root {
                return Err(format::invalid(
                    "identity already bound to another directory",
                ));
            }
        } else {
            if bound {
                return Err(format::invalid("binding has no registration"));
            }
            if state.paths.contains_key(&root) {
                return Err(format::invalid(
                    "path already registered with another identity",
                ));
            }
            if state.members.len() >= self.inner.options.max_databases as usize {
                return Err(Error::limit(
                    "shared_wal_databases",
                    state.members.len() as u64,
                    self.inner.options.max_databases.into(),
                ));
            }
            let embedded = self
                .inner
                .directory
                .path(Area::Root)
                .parent()
                .is_some_and(|parent| parent == root);
            let path = if embedded {
                ".."
            } else {
                root.to_str()
                    .ok_or_else(|| Error::invalid("database path", "requires UTF-8"))?
            };
            if path.len() > 4096 {
                return Err(Error::invalid("database path", "path is too long"));
            }
            self.charge(&mut state, super::member_charge(&root))?;
            let mut bytes = b"LSSR\x02\0\0\0".to_vec();
            bytes.extend_from_slice(&id.bytes());
            bytes.extend_from_slice(
                &u32::try_from(path.len())
                    .map_err(|_| format::invalid("path length"))?
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(path.as_bytes());
            if let Err(error) = publish_atomically(
                &self.inner.directory,
                Area::Root,
                &name(id),
                &format::checksum(bytes),
            ) {
                state.poison = true;
                return Err(error);
            }
            state.paths.insert(root.clone(), id);
            state.members.insert(
                id,
                Member {
                    root: root.clone(),
                    initialized: false,
                    retired: false,
                    checkpoint: 0,
                    latest: Pointer::default(),
                    seq: 0,
                    lsn: 0,
                    bytes: 0,
                    durable_seq: 0,
                    durable_pointer: Pointer::default(),
                    #[cfg(feature = "archive")]
                    archive: None,
                },
            );
        }
        if !bound
            && let Err(error) = publish_atomically(
                directory,
                Area::Root,
                "SHARED",
                &binding(self.inner.identity, id),
            )
        {
            state.poison = true;
            return Err(error);
        }
        Ok(())
    }
}
