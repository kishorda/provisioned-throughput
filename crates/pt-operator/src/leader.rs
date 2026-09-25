//! Leader election for the capacity controller (ADR-029).
//!
//! Replicas compete for a Kubernetes Lease (`pt_election`). Only the leader runs the
//! controller. A standby only follows the entitlement snapshot, so it's ready when it takes
//! over. A leader that can't renew stops reconciling at the renew deadline, before any
//! standby can take over, and the process exits so it restarts as a standby with fresh
//! caches. On SIGTERM the leader releases the lease, so a rolling update hands over at
//! once.
//!
//! Configured from the environment:
//!
//! | Variable | Default |
//! |----------|---------|
//! | `PT_LEADER_ELECTION` | `true`; `false` runs the controller without a lease (one replica only) |
//! | `PT_LEASE_NAMESPACE` | `$POD_NAMESPACE`, else `pt-system` |
//! | `PT_LEASE_NAME` | `pt-operator` |
//! | `POD_NAME` | identity; else `$HOSTNAME` |
//! | `PT_LEASE_DURATION_SECS` / `PT_RENEW_DEADLINE_SECS` / `PT_RETRY_PERIOD_SECS` | 15 / 10 / 2, as client-go |

use std::time::Duration;

use pt_election::ElectionConfig;

#[derive(Debug, Clone, PartialEq)]
pub struct LeaseSettings {
    pub namespace: String,
    pub name: String,
    pub election: ElectionConfig,
}

/// Leader-election settings from `env`, or `None` when election is turned off.
pub fn from_env(env: impl Fn(&str) -> Option<String>) -> anyhow::Result<Option<LeaseSettings>> {
    match env("PT_LEADER_ELECTION").as_deref() {
        None | Some("true") | Some("1") => {}
        Some("false") | Some("0") => return Ok(None),
        Some(other) => anyhow::bail!("PT_LEADER_ELECTION must be true or false, not {other}"),
    }
    let secs = |key: &str, default: u64| -> anyhow::Result<Duration> {
        match env(key) {
            None => Ok(Duration::from_secs(default)),
            Some(v) => Ok(Duration::from_secs(v.parse().map_err(|_| {
                anyhow::anyhow!("{key} must be a whole number of seconds, not {v}")
            })?)),
        }
    };
    let identity = env("POD_NAME")
        .or_else(|| env("HOSTNAME"))
        .ok_or_else(|| anyhow::anyhow!("set POD_NAME (or HOSTNAME) for leader election"))?;
    let election = ElectionConfig {
        identity,
        lease_duration: secs("PT_LEASE_DURATION_SECS", 15)?,
        renew_deadline: secs("PT_RENEW_DEADLINE_SECS", 10)?,
        retry_period: secs("PT_RETRY_PERIOD_SECS", 2)?,
    };
    // The controller hands out nothing that outlives it, so any gap will do.
    election
        .validate(Duration::ZERO)
        .map_err(anyhow::Error::msg)?;
    Ok(Some(LeaseSettings {
        namespace: env("PT_LEASE_NAMESPACE")
            .or_else(|| env("POD_NAMESPACE"))
            .unwrap_or_else(|| "pt-system".into()),
        name: env("PT_LEASE_NAME").unwrap_or_else(|| "pt-operator".into()),
        election,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(v: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = v
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn defaults_follow_client_go() {
        let s = from_env(env(&[
            ("POD_NAME", "pt-operator-7f9c"),
            ("POD_NAMESPACE", "pt"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(s.namespace, "pt");
        assert_eq!(s.name, "pt-operator");
        assert_eq!(s.election.identity, "pt-operator-7f9c");
        assert_eq!(
            (
                s.election.lease_duration,
                s.election.renew_deadline,
                s.election.retry_period
            ),
            (
                Duration::from_secs(15),
                Duration::from_secs(10),
                Duration::from_secs(2)
            )
        );
    }

    #[test]
    fn can_be_turned_off_and_is_validated() {
        assert!(from_env(env(&[("PT_LEADER_ELECTION", "false")]))
            .unwrap()
            .is_none());
        assert!(from_env(env(&[]))
            .unwrap_err()
            .to_string()
            .contains("POD_NAME"));
        let bad = from_env(env(&[("POD_NAME", "a"), ("PT_RENEW_DEADLINE_SECS", "20")]));
        assert!(bad.unwrap_err().to_string().contains("renew_deadline"));
        assert!(from_env(env(&[("PT_LEADER_ELECTION", "maybe")])).is_err());
    }
}
