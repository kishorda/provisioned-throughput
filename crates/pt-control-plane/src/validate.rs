//! Request validation that doesn't need capacity or storage.

use std::collections::HashSet;

use pt_admission::BoundaryPolicy;
use pt_core::{Shape, Tier};

use crate::config::{ControlPlaneConfig, ModelConfig};
use crate::model::{CreateRequest, RegionShare, Sku};
use crate::service::ServiceError;

const MAX_REGIONS: usize = 8;

fn invalid(field: &str, message: impl Into<String>) -> ServiceError {
    ServiceError::Validation {
        field: field.into(),
        message: message.into(),
    }
}

/// DNS-label style: 1–63 lowercase letters, digits, and hyphens, not starting or ending
/// with a hyphen.
pub fn name(name: &str) -> Result<(), ServiceError> {
    let ok = !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if ok {
        Ok(())
    } else {
        Err(invalid(
            "name",
            "Use 1–63 lowercase letters, digits, and hyphens, starting and ending with a letter or digit.",
        ))
    }
}

pub fn model<'a>(
    config: &'a ControlPlaneConfig,
    id: &str,
) -> Result<&'a ModelConfig, ServiceError> {
    config.model(id).ok_or_else(|| {
        invalid(
            "model",
            format!("Unknown model {id}. List models with GET /v1/models."),
        )
    })
}

pub fn tier(model: &ModelConfig, tier: Tier) -> Result<(), ServiceError> {
    if model.tiers.contains(&tier) {
        Ok(())
    } else {
        Err(invalid(
            "tier",
            format!("{} isn't offered at tier {tier:?}.", model.id),
        ))
    }
}

pub fn regions(
    config: &ControlPlaneConfig,
    model: &str,
    sku: Sku,
    regions: &[RegionShare],
) -> Result<(), ServiceError> {
    if regions.is_empty() || regions.len() > MAX_REGIONS {
        return Err(invalid(
            "regions",
            format!("Give between 1 and {MAX_REGIONS} regions."),
        ));
    }
    let offered = config.regions_for(model);
    let mut seen = HashSet::new();
    for r in regions {
        if !seen.insert(r.region.as_str()) {
            return Err(invalid(
                "regions",
                format!("Region {} is listed twice.", r.region),
            ));
        }
        // Minimum reservation is 1 CU.
        if r.cus < 1 {
            return Err(invalid(
                "regions",
                format!("Region {} needs at least 1 CU.", r.region),
            ));
        }
        if !offered.contains(&r.region.as_str()) {
            return Err(invalid(
                "regions",
                format!(
                    "{model} isn't offered in {}. Offered regions: {}.",
                    r.region,
                    offered.join(", ")
                ),
            ));
        }
    }
    if sku == Sku::MultiRegion && regions.len() < 2 {
        return Err(invalid(
            "sku",
            "A multi_region reservation needs at least two regions.",
        ));
    }
    if sku == Sku::MultiRegion {
        let zone = |r: &RegionShare| config.region(&r.region).and_then(|c| c.residency.clone());
        let first = zone(&regions[0]);
        if regions.iter().any(|r| zone(r) != first) {
            return Err(invalid(
                "regions",
                "A multi_region reservation's regions must share one data-residency zone, so failover keeps prompts inside it.",
            ));
        }
    }
    Ok(())
}

pub fn shape(model: &ModelConfig, s: &Shape) -> Result<(), ServiceError> {
    let fail = |m: String| Err(invalid("shape", m));
    if s.input_p95 > s.input_max {
        return fail("input_p95 can't exceed input_max.".into());
    }
    if s.input_max > s.context_ceiling {
        return fail("input_max can't exceed context_ceiling.".into());
    }
    if s.output_p95 == 0 || s.output_p95 > s.context_ceiling {
        return fail("output_p95 must be at least 1 and within context_ceiling.".into());
    }
    if s.context_ceiling > model.max_context {
        return fail(format!(
            "context_ceiling {} exceeds {}'s maximum context of {} tokens.",
            s.context_ceiling, model.id, model.max_context
        ));
    }
    if !(0.0..=1.0).contains(&s.cache_hit_ratio) {
        return fail("cache_hit_ratio must be between 0 and 1.".into());
    }
    if !(s.burst_factor >= 1.0 && s.burst_factor <= 20.0) {
        return fail("burst_factor must be between 1 and 20.".into());
    }
    Ok(())
}

pub fn boundary_policy(p: &BoundaryPolicy) -> Result<(), ServiceError> {
    let fail = |m: &str| Err(invalid("boundary_policy", m));
    if let Some(b) = &p.burst {
        if !(0.0..=3_600.0).contains(&b.max_credit_seconds) {
            return fail("burst.max_credit_seconds must be between 0 and 3600.");
        }
        if !(1.0..=10.0).contains(&b.max_rate_multiple) {
            return fail("burst.max_rate_multiple must be between 1 and 10.");
        }
        if !(0.0..=1.0).contains(&b.continuation_reserve) {
            return fail("burst.continuation_reserve must be between 0 and 1.");
        }
    }
    if let Some(q) = &p.queue {
        if q.deadline_ms == 0 || q.deadline_ms > 30_000 {
            return fail("queue.deadline_ms must be between 1 and 30000.");
        }
        if !(q.max_depth_wu_seconds > 0.0 && q.max_depth_wu_seconds <= 60.0) {
            return fail("queue.max_depth_wu_seconds must be between 0 and 60.");
        }
    }
    Ok(())
}

pub fn create(config: &ControlPlaneConfig, req: &CreateRequest) -> Result<(), ServiceError> {
    name(&req.name)?;
    let m = model(config, &req.model)?;
    tier(m, req.tier)?;
    regions(config, &req.model, req.sku, &req.regions)?;
    shape(m, &req.shape)?;
    boundary_policy(&req.boundary_policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        for ok in ["a", "acme-agents-prod", "x1"] {
            assert!(name(ok).is_ok(), "{ok}");
        }
        for bad in ["", "-a", "a-", "Acme", "a_b", &"a".repeat(64)] {
            assert!(name(bad).is_err(), "{bad}");
        }
    }
}
