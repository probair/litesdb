// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

pub(super) const NAME: &str = if cfg!(all(target_os = "linux", target_pointer_width = "64")) {
    "linux_clock_thread_cputime_id"
} else {
    "unavailable"
};

#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
#[allow(unsafe_code)]
pub(super) fn thread_cpu_ns() -> Option<u64> {
    use std::ffi::{c_int, c_long};
    #[repr(C)]
    struct Timespec {
        seconds: c_long,
        nanoseconds: c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: c_int, output: *mut Timespec) -> c_int;
    }
    let mut value = Timespec {
        seconds: 0,
        nanoseconds: 0,
    };
    if unsafe { clock_gettime(3, &raw mut value) } != 0
        || !(0..1_000_000_000).contains(&value.nanoseconds)
    {
        return None;
    }
    u64::try_from(value.seconds)
        .ok()?
        .checked_mul(1_000_000_000)?
        .checked_add(u64::try_from(value.nanoseconds).ok()?)
}

#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
pub(super) fn thread_cpu_ns() -> Option<u64> {
    None
}
