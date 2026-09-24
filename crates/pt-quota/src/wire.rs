//! Messages between gateways and the coordinator. JSON over HTTP for now; the shape maps
//! directly onto a gRPC service later.

use serde::{Deserialize, Serialize};

/// `POST /v1/leases/renew`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenewRequest {
    /// Stable for the life of a gateway process.
    pub gateway_id: String,
    pub reservations: Vec<ReservationDemand>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReservationDemand {
    pub id: String,
    /// The region's entitlement for this reservation, from the gateway's snapshot.
    pub entitlement_wu_s: f64,
    /// Snapshot version the entitlement came from. The newest version wins.
    pub snapshot_version: u64,
    /// Recent WU/s of admission attempts at this gateway, including rejected ones.
    pub demand_wu_s: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenewResponse {
    /// Leases are valid for this long after the gateway receives them.
    pub ttl_ms: u64,
    pub leases: Vec<Lease>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Lease {
    pub id: String,
    /// WU/s this gateway may admit for the reservation.
    pub rate_wu_s: f64,
    /// Gateways currently sharing the reservation. Used for the fallback share if the
    /// coordinator becomes unreachable.
    pub active_gateways: u32,
}
