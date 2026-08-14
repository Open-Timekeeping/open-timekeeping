//! Inbound (driving) ports for `timing-core`.
//!
//! The core implements every trait in this module; callers depend on the
//! trait rather than on the application service's concrete type. The
//! runtime drives the write side through [`EventAppendPort`]; the REST/SSE
//! API drives the read side through [`EventQueryPort`].
//!
//! Transport listeners are *not* here. An adapter that yields producer
//! sessions is implemented by the adapter and called by the runtime, which
//! makes it driven: see [`crate::ports::outbound::EventIngestPort`].

pub mod append;
pub mod query;

pub use append::{AppendError, AppendOutcome, EventAppendPort};
pub use query::{EventEntry, EventPage, EventQueryPort, EventStream, QueryError};
