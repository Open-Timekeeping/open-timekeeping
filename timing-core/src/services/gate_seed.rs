//! Startup recovery: rebuild [`SequenceGate`] state from the persisted log.
//!
//! Seeding reads through an outbound port ([`EventLog`]) to restore a piece
//! of domain state, which makes it an application concern rather than a
//! domain one: [`crate::domain`] stays free of port types, and the
//! composition root drives this the same way it drives every other
//! service-level operation.

use event_model::OtkEvent;
use tracing::{debug, info};

use crate::domain::SequenceGate;
use crate::ports::outbound::{EventLog, StorageError};

/// Replay every retained Detection in `log` through `gate.commit` so the
/// gate's per-`(producer_id, detector_id)` high-water marks reflect what
/// was actually persisted before this process started.
///
/// Returns the total number of entries scanned (Detections plus everything
/// else; the count is for logging, not correctness).
///
/// Call from the composition root after the log is opened and before any
/// ingest listener begins accepting connections. Without this seed, a
/// producer reconnecting after a node restart with the same `producer_id`
/// could replay sequences and have them silently re-persisted, defeating
/// the gate's idempotence guarantee.
///
/// # Memory cost
///
/// This implementation calls `read_range(earliest, None)` and so
/// materializes every retained entry into memory at once. That mirrors
/// the documented contract of [`EventLog::read_range`] and is acceptable
/// for v0 (a node with retention bounded to one race weekend holds at
/// most a few hundred thousand entries). A paginated variant is tracked
/// for the storage-layer follow-ups.
///
/// Non-Detection events (Crossing, Health, TimebaseStatus, Metadata) are
/// ignored: the gate is detection-keyed and nothing else carries the
/// `(producer_id, detector_id, sequence_number)` triple it needs.
pub async fn seed_from_log(
    gate: &SequenceGate,
    log: &mut dyn EventLog,
) -> Result<usize, StorageError> {
    let earliest = match log.earliest_offset().await? {
        Some(o) => o,
        None => {
            debug!("sequence_gate: log is empty; nothing to seed");
            return Ok(0);
        }
    };

    // `to = None` reads through the latest retained offset.
    let entries = log.read_range(earliest, None).await?;
    let scanned = entries.len();
    let mut detections_seeded = 0usize;

    for entry in entries {
        if let OtkEvent::Detection(det) = entry.event {
            gate.commit(&entry.producer_id, &det);
            detections_seeded += 1;
        }
    }

    info!(
        scanned,
        detections_seeded,
        from_offset = earliest.as_u64(),
        "sequence_gate: seeded from event log"
    );
    Ok(scanned)
}

/// Type-erased version of [`seed_from_log`] for the common case where
/// the caller holds the log as `&mut Box<dyn EventLog>` (the runtime's
/// composition shape). Forwards to [`seed_from_log`] via a single
/// reborrow so callers don't have to write the auto-deref incantation
/// at the call site.
pub async fn seed_from_log_box(
    gate: &SequenceGate,
    log: &mut Box<dyn EventLog>,
) -> Result<usize, StorageError> {
    seed_from_log(gate, log.as_mut()).await
}

#[cfg(test)]
mod tests {
    //! These tests exercise `seed_from_log` against the in-memory
    //! [`crate::testing::MockEventLog`]. The end-to-end "real backend +
    //! restart" behaviour is covered by `restart_resume_test` in
    //! `timing-node`; see the `testing` module's doc comment for why we
    //! don't dev-depend the real segment-log adapter here.

    use super::*;
    use event_model::{DetectorId, OtkEvent};

    use crate::domain::GateDecision;
    use crate::testing::{detection as det, MockEventLog};

    #[tokio::test]
    async fn seed_from_empty_log_is_noop() {
        let mut log = MockEventLog::new();
        let gate = SequenceGate::new();
        let scanned = seed_from_log(&gate, &mut log).await.unwrap();
        assert_eq!(scanned, 0);
        // Fresh gate behaviour is unchanged.
        assert_eq!(gate.check("p", &det("loop-1", 1)), GateDecision::Accept);
    }

    #[tokio::test]
    async fn seed_rebuilds_high_water_from_persisted_detections() {
        let mut log = MockEventLog::new();

        // Producer "p" wrote sequences 1, 2, 3 for loop-1; producer "q"
        // wrote sequence 5 for loop-2. After a restart the gate must
        // know about both keys with the correct high-water marks.
        log.append("p", &[OtkEvent::Detection(det("loop-1", 1))])
            .await
            .unwrap();
        log.append("p", &[OtkEvent::Detection(det("loop-1", 2))])
            .await
            .unwrap();
        log.append("p", &[OtkEvent::Detection(det("loop-1", 3))])
            .await
            .unwrap();
        log.append("q", &[OtkEvent::Detection(det("loop-2", 5))])
            .await
            .unwrap();

        let gate = SequenceGate::new();
        let scanned = seed_from_log(&gate, &mut log).await.unwrap();
        assert_eq!(scanned, 4);

        // Now drive the gate as if the producer reconnected after restart.
        // Replays of any sequence <= the persisted high water must be
        // rejected as duplicates; the next sequence must Advance.
        assert!(matches!(
            gate.check("p", &det("loop-1", 3)),
            GateDecision::Duplicate { high_water: 3, .. }
        ));
        assert!(matches!(
            gate.check("p", &det("loop-1", 4)),
            GateDecision::Advance {
                previous_high_water: 3
            }
        ));
        assert!(matches!(
            gate.check("q", &det("loop-2", 5)),
            GateDecision::Duplicate { high_water: 5, .. }
        ));
    }

    #[tokio::test]
    async fn seed_ignores_non_detection_events() {
        use event_model::{DetectorHealthEvent, DetectorHealthStatus};

        let mut log = MockEventLog::new();

        log.append(
            "p",
            &[OtkEvent::DetectorHealth(DetectorHealthEvent {
                detector_id: DetectorId::new("loop-1"),
                reported_at_ns: 0,
                status: DetectorHealthStatus::Healthy,
                message: None,
            })],
        )
        .await
        .unwrap();

        let gate = SequenceGate::new();
        let scanned = seed_from_log(&gate, &mut log).await.unwrap();
        // Scanned counts every entry, including the health event.
        assert_eq!(scanned, 1);
        // But the gate state is untouched: the next Detection is fresh.
        assert_eq!(gate.check("p", &det("loop-1", 1)), GateDecision::Accept);
    }
}
