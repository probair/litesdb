// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

mod fold;
mod head;

pub(crate) use fold::fold;
pub(crate) use head::{RetentionHead, RetentionHeads};
pub(crate) use head::{decode as decode_heads, head_name, publish as publish_heads};
