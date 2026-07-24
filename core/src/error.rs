// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{error, fmt, io};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Corruption,
    ResourceExhausted,
    InvalidArgument,
    Unsupported,
    Poisoned,
    Io,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CorruptionDetail {
    context: &'static str,
    reason: &'static str,
}

impl CorruptionDetail {
    #[must_use]
    pub const fn context(&self) -> &'static str {
        self.context
    }

    #[must_use]
    pub const fn reason(&self) -> &'static str {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LimitDetail {
    name: &'static str,
    actual: u64,
    maximum: u64,
}

impl LimitDetail {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn actual(&self) -> u64 {
        self.actual
    }

    #[must_use]
    pub const fn maximum(&self) -> u64 {
        self.maximum
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArgDetail {
    argument: &'static str,
    reason: &'static str,
}

impl ArgDetail {
    #[must_use]
    pub const fn argument(&self) -> &'static str {
        self.argument
    }

    #[must_use]
    pub const fn reason(&self) -> &'static str {
        self.reason
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpDetail {
    operation: &'static str,
    reason: &'static str,
}

impl OpDetail {
    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    #[must_use]
    pub const fn reason(&self) -> &'static str {
        self.reason
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    Corruption(CorruptionDetail),
    ResourceExhausted(LimitDetail),
    InvalidArgument(ArgDetail),
    Unsupported(OpDetail),
    Poisoned,
    Io(io::Error),
}

impl Error {
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::Corruption(_) => ErrorKind::Corruption,
            Self::ResourceExhausted(_) => ErrorKind::ResourceExhausted,
            Self::InvalidArgument(_) => ErrorKind::InvalidArgument,
            Self::Unsupported(_) => ErrorKind::Unsupported,
            Self::Poisoned => ErrorKind::Poisoned,
            Self::Io(_) => ErrorKind::Io,
        }
    }

    #[allow(dead_code, reason = "consumed by format decoders in later modules")]
    pub(crate) const fn corruption(context: &'static str, reason: &'static str) -> Self {
        Self::Corruption(CorruptionDetail { context, reason })
    }

    #[allow(dead_code, reason = "consumed by the limits module next")]
    pub(crate) const fn limit(name: &'static str, actual: u64, maximum: u64) -> Self {
        Self::ResourceExhausted(LimitDetail {
            name,
            actual,
            maximum,
        })
    }

    #[allow(dead_code, reason = "consumed by the types module next")]
    pub(crate) const fn invalid(argument: &'static str, reason: &'static str) -> Self {
        Self::InvalidArgument(ArgDetail { argument, reason })
    }

    #[allow(dead_code, reason = "consumed by codec selection in a later milestone")]
    pub(crate) const fn unsupported(operation: &'static str, reason: &'static str) -> Self {
        Self::Unsupported(OpDetail { operation, reason })
    }
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::Io(source)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corruption(detail) => write!(
                formatter,
                "corruption while decoding {}: {}",
                detail.context, detail.reason
            ),
            Self::ResourceExhausted(detail) => write!(
                formatter,
                "resource limit {} exceeded: {} > {}",
                detail.name, detail.actual, detail.maximum
            ),
            Self::InvalidArgument(detail) => write!(
                formatter,
                "invalid argument {}: {}",
                detail.argument, detail.reason
            ),
            Self::Unsupported(detail) => write!(
                formatter,
                "unsupported operation {}: {}",
                detail.operation, detail.reason
            ),
            Self::Poisoned => formatter.write_str("database writer is poisoned; reopen required"),
            Self::Io(source) => write!(formatter, "I/O error: {source}"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            Self::Corruption(_)
            | Self::ResourceExhausted(_)
            | Self::InvalidArgument(_)
            | Self::Unsupported(_)
            | Self::Poisoned => None,
        }
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
