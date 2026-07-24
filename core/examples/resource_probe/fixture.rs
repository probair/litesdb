// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::error::Error;

pub(super) const CADENCE_SECONDS: u64 = 3;
pub(super) const DAY_SECONDS: u64 = 86_400;
pub(super) const START_TIMESTAMP: i64 = 1_700_000_000;
pub(super) const STREAM_COUNT: u64 = 14;
pub(super) const DIGEST_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

const TEN_MB: u64 = 10_000_000;
const MEM_CAPACITY_BYTES: u64 = 32_000_000_000;
const DISK_CAPACITY_BYTES: u64 = 1_000_000_000_000;
const SAMPLES_PER_DAY: u64 = DAY_SECONDS / CADENCE_SECONDS;
const RESTART_CYCLE_SAMPLES: u64 = 30 * SAMPLES_PER_DAY;
const RESTART_OFFSETS: [u64; 3] = [
    5 * SAMPLES_PER_DAY + 11_880 / CADENCE_SECONDS,
    15 * SAMPLES_PER_DAY + 67_320 / CADENCE_SECONDS,
    24 * SAMPLES_PER_DAY + 25_500 / CADENCE_SECONDS,
];

pub(super) struct HostSample {
    pub(super) timestamp: i64,
    pub(super) cpu_sq1: u8,
    pub(super) load_x100: [u64; 3],
    pub(super) mem_usage_bytes: u64,
    pub(super) mem_capacity_bytes: u64,
    pub(super) disk_usage_bytes: u64,
    pub(super) disk_capacity_bytes: u64,
    pub(super) net_rx_bytes: u64,
    pub(super) net_tx_bytes: u64,
    pub(super) boot_timestamp: u64,
    pub(super) ping_ms: [Option<u64>; 3],
    pub(super) ping_due: bool,
    pub(super) disk_due: bool,
    pub(super) initial: bool,
    pub(super) restart: bool,
}

pub(super) struct HostFixture {
    index: u64,
    mem_usage_units: u64,
    disk_raw_bytes: u64,
    disk_reported_bytes: u64,
    net_rx_raw_bytes: u64,
    net_tx_raw_bytes: u64,
    boot_timestamp: u64,
    load_milli: [i64; 3],
    ping_ms: [Option<u64>; 3],
    last_restart_index: Option<u64>,
}

impl HostFixture {
    pub(super) fn new() -> Self {
        Self {
            index: 0,
            mem_usage_units: 960,
            disk_raw_bytes: 320_000_000_000,
            disk_reported_bytes: 320_000_000_000,
            net_rx_raw_bytes: 8_000_000_000_000,
            net_tx_raw_bytes: 3_000_000_000_000,
            boot_timestamp: START_TIMESTAMP.unsigned_abs() - 9 * DAY_SECONDS,
            load_milli: [800; 3],
            ping_ms: [None; 3],
            last_restart_index: None,
        }
    }

    pub(super) fn next(&mut self) -> Result<HostSample, Box<dyn Error>> {
        let index = self.index;
        let offset = index
            .checked_mul(CADENCE_SECONDS)
            .ok_or("fixture timestamp offset overflow")?;
        let timestamp = START_TIMESTAMP
            .checked_add(i64::try_from(offset)?)
            .ok_or("fixture timestamp overflow")?;
        let restart = is_restart(index);
        if restart {
            self.boot_timestamp = timestamp.unsigned_abs();
            self.mem_usage_units = 520 + mix(index ^ 0x7265_7374_6172_7401) % 101;
            self.last_restart_index = Some(index);
        } else if index > 0 && index.is_multiple_of(400) {
            self.jump_memory(index);
        }

        let cpu_basis_points = cpu_basis_points(index, self.last_restart_index);
        self.update_load(cpu_basis_points);
        if index > 0 {
            self.update_counters(index, cpu_basis_points)?;
        }

        let ping_due = index.is_multiple_of(5);
        if ping_due {
            self.ping_ms = ping_values(index, cpu_basis_points, restart);
        }
        let disk_due = index.is_multiple_of(20);
        if disk_due {
            self.disk_reported_bytes = quantize_10mb(self.disk_raw_bytes);
        }
        let sample = HostSample {
            timestamp,
            cpu_sq1: sq1_code(cpu_basis_points),
            load_x100: self.load_x100(),
            mem_usage_bytes: self
                .mem_usage_units
                .checked_mul(TEN_MB)
                .ok_or("memory usage overflow")?,
            mem_capacity_bytes: MEM_CAPACITY_BYTES,
            disk_usage_bytes: self.disk_reported_bytes,
            disk_capacity_bytes: DISK_CAPACITY_BYTES,
            net_rx_bytes: quantize_10mb(self.net_rx_raw_bytes),
            net_tx_bytes: quantize_10mb(self.net_tx_raw_bytes),
            boot_timestamp: self.boot_timestamp,
            ping_ms: self.ping_ms,
            ping_due,
            disk_due,
            initial: index == 0,
            restart,
        };
        self.index = self.index.checked_add(1).ok_or("fixture index overflow")?;
        Ok(sample)
    }

    fn jump_memory(&mut self, index: u64) {
        let entropy = mix(index ^ 0x6d65_6d6f_7279_0001);
        let mut delta = i64::try_from(entropy % 61).unwrap_or_default() - 20;
        if (entropy >> 8).is_multiple_of(8) {
            delta -= 80;
        }
        let next = i64::try_from(self.mem_usage_units)
            .unwrap_or(2_800)
            .saturating_add(delta)
            .clamp(450, 2_800);
        self.mem_usage_units = u64::try_from(next).unwrap_or(450);
    }

    fn update_load(&mut self, cpu_basis_points: u64) {
        let target_milli =
            i64::try_from(cpu_basis_points.saturating_mul(8) / 10).unwrap_or(i64::MAX);
        for (load, divisor) in self.load_milli.iter_mut().zip([20_i64, 100, 300]) {
            *load = load.saturating_add((target_milli - *load) / divisor);
        }
    }

    fn update_counters(&mut self, index: u64, cpu_basis_points: u64) -> Result<(), Box<dyn Error>> {
        let entropy = mix(index ^ 0x6e65_7477_6f72_6b01);
        let rx_delta = 100_000_u64
            .checked_add(
                cpu_basis_points
                    .checked_mul(180)
                    .ok_or("RX rate overflow")?,
            )
            .and_then(|value| value.checked_add(entropy % 500_001))
            .ok_or("RX delta overflow")?;
        let tx_delta = 50_000_u64
            .checked_add(cpu_basis_points.checked_mul(70).ok_or("TX rate overflow")?)
            .and_then(|value| value.checked_add((entropy >> 24) % 250_001))
            .ok_or("TX delta overflow")?;
        self.net_rx_raw_bytes = self
            .net_rx_raw_bytes
            .checked_add(rx_delta)
            .ok_or("RX counter overflow")?;
        self.net_tx_raw_bytes = self
            .net_tx_raw_bytes
            .checked_add(tx_delta)
            .ok_or("TX counter overflow")?;
        let disk_delta = rx_delta
            .checked_add(tx_delta)
            .and_then(|value| value.checked_div(25))
            .and_then(|value| value.checked_add(20_000))
            .ok_or("disk growth overflow")?;
        self.disk_raw_bytes = self
            .disk_raw_bytes
            .checked_add(disk_delta)
            .ok_or("disk counter overflow")?
            .min(DISK_CAPACITY_BYTES - TEN_MB);
        Ok(())
    }

    fn load_x100(&self) -> [u64; 3] {
        self.load_milli
            .map(|value| u64::try_from(value.saturating_add(5) / 10).unwrap_or_default())
    }
}

fn quantize_10mb(bytes: u64) -> u64 {
    bytes / TEN_MB * TEN_MB
}

pub(super) fn update_dense_digest(mut digest: u64, sample: &HostSample) -> u64 {
    let values = [
        sample.timestamp.unsigned_abs(),
        1,
        u64::from(sample.cpu_sq1),
        sample.load_x100[0],
        sample.load_x100[1],
        sample.load_x100[2],
        sample.mem_usage_bytes,
        sample.mem_capacity_bytes,
        sample.disk_usage_bytes,
        sample.disk_capacity_bytes,
        sample.net_rx_bytes,
        sample.net_tx_bytes,
        sample.boot_timestamp,
        sample.ping_ms[0].unwrap_or(u64::MAX),
        sample.ping_ms[1].unwrap_or(u64::MAX),
        sample.ping_ms[2].unwrap_or(u64::MAX),
    ];
    for value in values {
        for byte in value.to_le_bytes() {
            digest ^= u64::from(byte);
            digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    digest
}

fn cpu_basis_points(index: u64, last_restart: Option<u64>) -> u64 {
    let day = index / SAMPLES_PER_DAY;
    let second = index % SAMPLES_PER_DAY * CADENCE_SECONDS;
    let mut base = match second {
        0..18_000 => 1_000,
        18_000..32_400 => lerp(1_000, 6_200, second - 18_000, 14_400),
        32_400..43_200 => 6_200,
        43_200..50_400 => 3_800,
        50_400..64_800 => 6_800,
        64_800..75_600 => 8_500,
        _ => lerp(8_500, 1_200, second - 75_600, 10_800),
    };
    if day % 7 >= 5 {
        base = base * 3 / 4;
    }
    let entropy = mix(index ^ 0x6370_755f_6a75_6d70);
    let noise = i64::try_from(entropy % 1_201).unwrap_or_default() - 600;
    let shock = if (entropy >> 16) % 100 < 7 {
        i64::try_from((entropy >> 24) % 5_001).unwrap_or_default() - 2_500
    } else {
        0
    };
    let boot_spike = last_restart
        .and_then(|restart| index.checked_sub(restart))
        .filter(|since| *since < 100)
        .map_or(0, |since| i64::try_from((100 - since) * 25).unwrap_or(0));
    (base + noise + shock + boot_spike).clamp(100, 9_900) as u64
}

fn ping_values(index: u64, cpu_basis_points: u64, restart: bool) -> [Option<u64>; 3] {
    if restart {
        return [None; 3];
    }
    let second = index % SAMPLES_PER_DAY * CADENCE_SECONDS;
    let ping_index = index / 5;
    let bases = [105_i64, 45, 155];
    let jitters = [8_u64, 4, 12];
    std::array::from_fn(|route| {
        let entropy = mix(ping_index ^ (0x7069_6e67_0000_0001 + route as u64));
        let loss = loss_percent(route, second);
        if entropy % 100 < loss {
            None
        } else {
            let width = jitters[route];
            let jitter = i64::try_from((entropy >> 8) % (width * 2 + 1)).unwrap_or_default()
                - i64::try_from(width).unwrap_or_default();
            let queue = i64::try_from(cpu_basis_points.saturating_sub(6_000) / 250)
                .unwrap_or_default()
                * if route == 2 { 2 } else { 1 };
            u64::try_from((bases[route] + jitter + queue).max(1)).ok()
        }
    })
}

fn loss_percent(route: usize, second: u64) -> u64 {
    let peak = (64_800..75_600).contains(&second);
    let busy = (32_400..64_800).contains(&second);
    match (route, peak, busy) {
        (0, true, _) => 4,
        (0, false, true) => 2,
        (0, false, false) => 1,
        (1, true, _) => 12,
        (1, false, true) => 7,
        (1, false, false) => 4,
        (2, true, _) => 20,
        (2, false, true) => 15,
        (2, false, false) => 10,
        _ => 20,
    }
}

fn sq1_code(basis_points: u64) -> u8 {
    let code = if basis_points <= 500 {
        (basis_points + 5) / 10
    } else if basis_points <= 1_200 {
        50 + (basis_points - 500 + 12) / 25
    } else {
        78 + (basis_points - 1_200 + 25) / 50
    };
    u8::try_from(code.min(254)).unwrap_or(254)
}

fn is_restart(index: u64) -> bool {
    RESTART_OFFSETS.contains(&(index % RESTART_CYCLE_SAMPLES))
}

fn lerp(start: i64, end: i64, position: u64, span: u64) -> i64 {
    start
        + (end - start) * i64::try_from(position).unwrap_or_default()
            / i64::try_from(span).unwrap_or(1)
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realistic_month_preserves_host_invariants() {
        let mut fixture = HostFixture::new();
        let mut previous_rx = 0_u64;
        let mut previous_tx = 0_u64;
        let mut previous_disk = 0_u64;
        let mut previous_memory = 0_u64;
        let mut memory_jumps = 0_u64;
        let mut restarts = 0_u64;
        let mut ping_samples = 0_u64;
        let mut ping_nulls = [0_u64; 3];
        let mut min_cpu = u8::MAX;
        let mut max_cpu = u8::MIN;

        for _ in 0..30 * SAMPLES_PER_DAY {
            let sample = fixture.next().unwrap_or_else(|_| unreachable!());
            assert_eq!(sample.mem_usage_bytes % TEN_MB, 0);
            assert_eq!(sample.disk_usage_bytes % TEN_MB, 0);
            assert_eq!(sample.net_rx_bytes % TEN_MB, 0);
            assert_eq!(sample.net_tx_bytes % TEN_MB, 0);
            assert!(sample.load_x100.iter().all(|value| *value <= 800));
            assert_eq!(sample.mem_capacity_bytes, MEM_CAPACITY_BYTES);
            assert_eq!(sample.disk_capacity_bytes, DISK_CAPACITY_BYTES);
            assert!(sample.net_rx_bytes >= previous_rx);
            assert!(sample.net_tx_bytes >= previous_tx);
            assert!(sample.disk_usage_bytes >= previous_disk);
            if previous_disk != 0 && sample.disk_usage_bytes != previous_disk {
                assert!(sample.disk_due);
            }
            if previous_memory != 0 && sample.mem_usage_bytes != previous_memory {
                memory_jumps = memory_jumps.saturating_add(1);
            }
            if sample.restart {
                restarts = restarts.saturating_add(1);
            }
            if sample.ping_due {
                ping_samples = ping_samples.saturating_add(1);
                for (count, value) in ping_nulls.iter_mut().zip(sample.ping_ms) {
                    if value.is_none() {
                        *count = count.saturating_add(1);
                    }
                }
            }
            min_cpu = min_cpu.min(sample.cpu_sq1);
            max_cpu = max_cpu.max(sample.cpu_sq1);
            previous_rx = sample.net_rx_bytes;
            previous_tx = sample.net_tx_bytes;
            previous_disk = sample.disk_usage_bytes;
            previous_memory = sample.mem_usage_bytes;
        }

        assert_eq!(restarts, 3);
        assert_eq!(ping_samples, 172_800);
        assert!(memory_jumps > 100);
        assert!(max_cpu.saturating_sub(min_cpu) > 150);
        assert!(previous_disk > 320_000_000_000);
        assert!(ping_nulls[0] < ping_nulls[1]);
        assert!(ping_nulls[1] < ping_nulls[2]);
        for nulls in ping_nulls {
            let loss_percent = nulls.saturating_mul(100) / ping_samples;
            assert!((1..=20).contains(&loss_percent));
        }
    }
}
