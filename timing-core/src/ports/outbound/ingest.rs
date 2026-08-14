//! Outbound port: producer sessions supplied by transport adapters.
//!
//! Per-transport adapters (e.g. `adapter-ingest-tcp`,
//! `adapter-ingest-unix-socket`) implement the traits in this module and own
//! all framing, decoding, and handshake mechanics. The runtime drives them:
//! it calls [`EventIngestPort::accept`] to obtain sessions and polls
//! [`IngestSession::next_event`] for canonical events, which it then feeds
//! to the inbound [`crate::ports::inbound::EventAppendPort`].
//!
//! # Why this is an outbound port
//!
//! Data flows inward through it, but the *dependency* points outward: the
//! core never implements these traits, it calls them, and the composition
//! root injects the concrete adapter. That makes it driven, the same
//! direction as [`crate::ports::outbound::EventLog`]. The port that the
//! core itself implements for the write path is
//! [`crate::ports::inbound::EventAppendPort`].
//!
//! # Roles
//!
//! - [`EventIngestPort`]: listener; accept sessions from producers.
//! - [`IngestSession`]: a single connected producer; poll
//!   [`IngestSession::next_event`] until done.
//! - [`IngestError`]: error vocabulary for both accept and session operations.

use async_trait::async_trait;
use event_model::OtkEvent;
use thiserror::Error;

/// Errors that can surface from [`EventIngestPort::accept`] or
/// [`IngestSession::next_event`].
///
/// The vocabulary is deliberately transport-neutral: it names what the core
/// has to react to, not how any particular link failed. Adapters map their
/// own mechanism-specific failures (`std::io::Error`, framing errors, TLS
/// handshake failures, USB stalls) onto these variants and carry the
/// original detail in the payload string, so a serial or in-process adapter
/// is not forced to invent socket semantics it does not have.
#[derive(Debug, Error)]
pub enum IngestError {
    /// The port will accept no further sessions. The runtime stops its
    /// accept loop for this listener.
    #[error("port is closed")]
    Closed,

    /// A peer did not establish a usable session: refused, unauthorised, or
    /// the handshake did not complete.
    #[error("session rejected: {0}")]
    Rejected(String),

    /// A peer delivered input that could not be turned into a canonical
    /// event. The peer is at fault; the session is not salvageable.
    #[error("malformed input: {0}")]
    Malformed(String),

    /// The adapter's underlying mechanism failed (link dropped, bind
    /// refused, device disappeared, misconfiguration detected at startup).
    #[error("transport failure: {0}")]
    Transport(String),
}

/// One event delivered up from an [`IngestSession`].
///
/// Carries the canonical [`OtkEvent`] plus any per-message correlation
/// metadata the transport learned during decode but the event itself
/// doesn't carry. Today that means just the optional W3C `traceparent`
/// from the envelope (already format-validated upstream by
/// `ingest-protocol`).
#[derive(Debug, Clone)]
pub struct IncomingEvent {
    pub event: OtkEvent,
    /// W3C Trace Context `traceparent` value from the envelope, when the
    /// producer set one and it passed validation. Consumers use this to
    /// parent the per-event tracing span on the producer's trace so logs
    /// stitch across the wire in any OpenTelemetry-aware backend.
    pub traceparent: Option<String>,
}

/// A single connected producer session.
///
/// Call `next_event` in a loop to receive typed events. Returns `None` when
/// the producer disconnects cleanly. Returns `Err` on a terminal error.
#[async_trait]
pub trait IngestSession: Send {
    async fn next_event(&mut self) -> Result<Option<IncomingEvent>, IngestError>;

    /// The producer identity established during the handshake. Stable for
    /// the lifetime of the session.
    fn producer_id(&self) -> &str;

    /// An operator-facing label for the far end of this session, when the
    /// transport has a meaningful one (a socket peer address, a device
    /// path). Used for logging and diagnostics only, never for identity or
    /// routing decisions.
    ///
    /// Returns `None` for transports with no addressable remote (in-process
    /// plugins, replay sources), which is why this is not a required
    /// `&str`: no adapter should have to fabricate an address to satisfy
    /// the port.
    fn remote_label(&self) -> Option<&str>;
}

/// Listener that yields producer sessions.
///
/// Each call to `accept` suspends until the next producer connects and
/// completes the OTK handshake, then returns a ready [`IngestSession`]. The
/// caller drives `next_event` on the session until it returns `None` (clean
/// disconnect) or `Err` (terminal error).
///
/// Framing, decoding, and handshake mechanics are adapter concerns and are
/// not visible through this port.
#[async_trait]
pub trait EventIngestPort: Send + Sync {
    async fn accept(&self) -> Result<Box<dyn IngestSession>, IngestError>;
}
