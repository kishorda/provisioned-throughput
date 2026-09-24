//! Tenant-aware router (docs/05 §3, docs/13).
//!
//! Sits between the PT gateway and inference workers. It decides *which request goes next*
//! (priority classes, then WFQ by WU) and *which worker serves it* (pull-based dispatch on
//! free slots and KV blocks, prefix and session affinity, placement filters, per-tenant KV
//! budgets). In production these decisions move into Dynamo's router: see docs/13 for the
//! mapping onto Dynamo's plugins.

pub mod config;
pub mod dispatch;
pub mod http;
pub mod scheduler;
pub mod workers;
