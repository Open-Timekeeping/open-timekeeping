//! Ports for `timing-core`: the hexagon's edge.
//!
//! Ports are the interfaces the core declares on its boundary. The split is
//! by *who implements*, not by which way data travels: the core implements
//! every [`inbound`] port, and adapters implement every [`outbound`] port,
//! with the concrete impl injected at construction.
//!
//! That is why a transport listener ([`outbound::EventIngestPort`]) is
//! outbound even though producer traffic flows inward through it: the
//! adapter implements it and the runtime calls it. The write path the core
//! itself implements is [`inbound::EventAppendPort`].
//!
//! Adapter crates and the runtime composition root depend on these types
//! and on nothing else from `timing-core`. The domain (`crate::domain`) and
//! the application services (`crate::services`) live behind these
//! interfaces; adapters must not reach for them directly.
//!
//! That property used to be enforced by Cargo's dependency graph (each port
//! lived in its own crate so adapters could declare only the port crate as
//! a dependency). When ports moved into `timing-core` the dep-graph fence
//! went away, replaced by a per-adapter `clippy.toml` that denies
//! `timing_core::domain::*` and `timing_core::services::*` imports outside
//! of the runtime composition root. The shape of the property is
//! unchanged; only the enforcement mechanism is.

pub mod inbound;
pub mod outbound;

// Convenience re-exports so adapters can write `use timing_core::ports::EventLog`
// without naming the inbound/outbound split.
pub use inbound::{
    AppendError, AppendOutcome, EventAppendPort, EventEntry, EventPage, EventQueryPort,
    EventStream, QueryError,
};
pub use outbound::{
    EventIngestPort, EventLog, IncomingEvent, IngestError, IngestMetrics, IngestSession, LogEntry,
    LogSubscription, NoopIngestMetrics, Offset, RetentionPolicy, StorageError,
};
