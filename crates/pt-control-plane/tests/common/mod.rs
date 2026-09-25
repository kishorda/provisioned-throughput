//! Shared fixtures. Tests use the committed example config, so it's always valid.

#![allow(dead_code)]

use std::path::PathBuf;

use jiff::Timestamp;
use pt_control_plane::model::{CreateRequest, RegionShare, Sku};
use pt_control_plane::ControlPlaneConfig;
use pt_core::{PoolIsolation, Shape, TermMonths, Tier};

pub const ACME: &str = "acme";
pub const GLOBEX: &str = "globex";
pub const ACME_KEY: &str = "sk-admin-acme-dev";
pub const MAVERICK: &str = "llama-4-maverick";
/// Standard base price from the example config.
pub const BASE: u64 = 150_000;

pub fn config() -> ControlPlaneConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/control-plane.toml");
    ControlPlaneConfig::load(path).expect("example config is valid")
}

pub fn t0() -> Timestamp {
    "2026-10-01T00:00:00Z".parse().unwrap()
}

pub fn shares(v: &[(&str, u32)]) -> Vec<RegionShare> {
    v.iter()
        .map(|(r, c)| RegionShare {
            region: r.to_string(),
            cus: *c,
        })
        .collect()
}

pub fn shape(context_ceiling: u64) -> Shape {
    Shape {
        input_p95: 4_000,
        input_max: 16_000.min(context_ceiling),
        output_p95: 500,
        context_ceiling,
        cache_hit_ratio: 0.5,
        burst_factor: 2.0,
    }
}

pub fn request(name: &str, regions: &[(&str, u32)]) -> CreateRequest {
    CreateRequest {
        name: name.into(),
        model: MAVERICK.into(),
        tier: Tier::Agentic,
        regions: shares(regions),
        sku: Sku::Regional,
        isolation: PoolIsolation::Shared,
        term_months: TermMonths::One,
        start_at: None,
        auto_renew: true,
        shape: shape(32_768),
        boundary_policy: Default::default(),
        rebalance: true,
    }
}
