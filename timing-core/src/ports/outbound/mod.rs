//! Outbound (driven) ports for `timing-core`.
//!
//! The core depends on these interfaces; the composition root supplies
//! concrete implementations at construction time. Adapter crates own the
//! concrete impls (`adapter-event-log-segment` for [`EventLog`];
//! `adapter-ingest-tcp` and `adapter-ingest-unix-socket` for
//! [`EventIngestPort`]; `timing-node`'s Prometheus `Metrics` for
//! [`IngestMetrics`]).

pub mod event_log;
pub mod ingest;
pub mod metrics;

pub use event_log::{EventLog, LogEntry, LogSubscription, Offset, RetentionPolicy, StorageError};
pub use ingest::{EventIngestPort, IncomingEvent, IngestError, IngestSession};
pub use metrics::{IngestMetrics, NoopIngestMetrics};
