//! Application services.
//!
//! Services in this module orchestrate the domain (`crate::domain`) against
//! injected outbound ports (`crate::ports::outbound`) and expose their
//! public surface via inbound ports (`crate::ports::inbound`). Composition
//! roots (`timing-node` at v0; an offline analyzer or replay tool later)
//! build the adapters, construct one of these services, and route inbound
//! traffic to it.
//!
//! At v0 there is one service, [`EventIngestService`], which owns the
//! end-to-end peek/append/commit dance for incoming events, plus
//! [`seed_from_log`], the startup routine that restores sequence-gate
//! state from the persisted log.

pub mod event_ingest;
pub mod gate_seed;

pub use event_ingest::EventIngestService;
pub use gate_seed::{seed_from_log, seed_from_log_box};
