// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{error::Error as _, io};

use super::{Error, ErrorKind};

#[test]
fn categories_and_details_are_stable() {
    let corruption = Error::corruption("WAL record", "CRC mismatch");
    assert_eq!(corruption.kind(), ErrorKind::Corruption);
    let Error::Corruption(detail) = corruption else {
        unreachable!();
    };
    assert_eq!(detail.context(), "WAL record");
    assert_eq!(detail.reason(), "CRC mismatch");

    let limit = Error::limit("record payload", 65_537, 65_536);
    assert_eq!(limit.kind(), ErrorKind::ResourceExhausted);
    let Error::ResourceExhausted(detail) = limit else {
        unreachable!();
    };
    assert_eq!(
        (detail.name(), detail.actual(), detail.maximum()),
        ("record payload", 65_537, 65_536)
    );

    let invalid = Error::invalid("timestamp", "must increase");
    assert_eq!(invalid.kind(), ErrorKind::InvalidArgument);
    let Error::InvalidArgument(detail) = invalid else {
        unreachable!();
    };
    assert_eq!(
        (detail.argument(), detail.reason()),
        ("timestamp", "must increase")
    );

    let unsupported = Error::unsupported("codec", "unknown tag");
    assert_eq!(unsupported.kind(), ErrorKind::Unsupported);
    let Error::Unsupported(detail) = unsupported else {
        unreachable!();
    };
    assert_eq!(
        (detail.operation(), detail.reason()),
        ("codec", "unknown tag")
    );
}

#[test]
fn display_includes_actionable_context() {
    assert_eq!(
        Error::corruption("manifest", "bad magic").to_string(),
        "corruption while decoding manifest: bad magic"
    );
    assert_eq!(
        Error::limit("table count", 4, 3).to_string(),
        "resource limit table count exceeded: 4 > 3"
    );
    assert_eq!(
        Error::invalid("entries", "empty").to_string(),
        "invalid argument entries: empty"
    );
    assert_eq!(
        Error::Poisoned.to_string(),
        "database writer is poisoned; reopen required"
    );
}

#[test]
fn io_error_preserves_its_source() {
    let error = Error::from(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("denied")
    );
}
