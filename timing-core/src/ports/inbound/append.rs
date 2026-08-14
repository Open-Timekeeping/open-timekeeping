//! Inbound port: the write half of the ingest pipeline.
//!
//! This is the typed boundary between the runtime's listener loop and the
//! application service that owns the pipeline
//! ([`crate::services::EventIngestService`] at v0). The runtime holds an
//! `Arc<dyn EventAppendPort>` and depends on nothing else from the service
//! module, so an alternative core (a replay tool, an offline analyzer, a
//! conformance double) substitutes at the composition root the same way
//! [`crate::ports::inbound::EventQueryPort`] already allows on the read
//! side.
//!
//! # Roles
//!
//! - [`EventAppendPort`]: submit one canonical event on behalf of a producer.
//! - [`AppendOutcome`]: what the pipeline did with it.
//! - [`AppendError`]: caller-shaped error vocabulary. Storage backend errors
//!   are mapped to these variants at the service boundary so the runtime
//!   never sees implementation details of any particular event-log backend.

use async_trait::async_trait;
use event_model::OtkEvent;
use thiserror::Error;

/// Outcome of a successful [`EventAppendPort::append_event`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The event (and any derived crossings) were persisted. The `u64` is
    /// the log position of the last record in the batch; the submitted
    /// event's own position is recoverable as `offset - n_crossings`.
    ///
    /// Positions cross this boundary as plain `u64`, matching
    /// [`crate::ports::inbound::EventPage`] on the read side. The
    /// [`Offset`](crate::ports::outbound::Offset) newtype belongs to the
    /// storage port and stops there.
    Appended(u64),

    /// The event was a duplicate per the sequence gate and was dropped
    /// without persisting. The runtime treats this as success at the
    /// boundary so the producer's session stays open.
    DroppedDuplicate,
}

/// Caller-shaped error vocabulary for the append port.
///
/// Two variants, split by who has to act: [`Self::Rejected`] means the
/// submitted event will never be accepted and the caller should stop
/// re-sending it; [`Self::Unavailable`] means the core could not service
/// the request and a retry may succeed. Anything the storage backend
/// reports collapses into one of those two at the service boundary.
#[derive(Debug, Error)]
pub enum AppendError {
    /// The event itself is unacceptable. Re-submitting the same event will
    /// fail the same way.
    #[error("event rejected: {0}")]
    Rejected(String),

    /// The core could not complete the append. The detail string is for
    /// server-side logging; it may name backend specifics and should not be
    /// echoed to producers.
    #[error("append unavailable: {0}")]
    Unavailable(String),
}

/// Write-side inbound port: submit canonical events into the pipeline.
#[async_trait]
pub trait EventAppendPort: Send + Sync {
    /// Append one canonical event attributed to `producer_id`.
    ///
    /// Duplicate detections are dropped rather than rejected: the caller
    /// gets `Ok(`[`AppendOutcome::DroppedDuplicate`]`)`, because a producer
    /// re-sending after a reconnect is behaving correctly, not erring.
    async fn append_event(
        &self,
        producer_id: &str,
        event: OtkEvent,
    ) -> Result<AppendOutcome, AppendError>;
}
