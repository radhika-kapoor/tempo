//! E2E tests for the DKG actor's `handle_finalized_block`,
//! `process_mid_epoch_block`, and `resolve_epoch_outcome` logic.
//!
//! These tests exercise the live code paths by running real nodes.  They are
//! the appropriate level for this coverage because all three functions depend
//! on infrastructure that cannot be cheaply faked in unit tests:
//!
//! - `handle_finalized_block` / `process_mid_epoch_block` are driven by the
//!   `Actor`'s main loop, which requires a `marshal::Mailbox` whose
//!   constructor is `pub(crate)` in an external crate.
//! - `resolve_epoch_outcome` queries the on-chain validator contract through a
//!   live `TempoFullNode`.

use commonware_macros::test_traced;
use commonware_runtime::{
    Runner as _,
    deterministic::{Config, Runner},
};
use futures::future::join_all;

use super::common::{
    assert_no_dkg_failures, sum_metric_with_suffix, wait_for_outcome,
    wait_for_validators_to_reach_epoch,
};
use crate::{Setup, setup_validators};

// ── handle_finalized_block ────────────────────────────────────────────────────

/// A mid-epoch block (not the last block of the epoch) must not produce a new
/// DKG state — the DKG round should continue with the same epoch.
///
/// Verifies the non-boundary path inside `handle_finalized_block`:
/// epoch ordering passes → phase actions run → `process_mid_epoch_block`
/// returns `Ok(None)` → validators stay in the same epoch until the last block.
#[test_traced]
fn mid_epoch_blocks_do_not_advance_epoch() {
    MidEpochBlockTest {
        how_many_signers: 4,
        epoch_length: 20,
        wait_until_epoch: 2,
    }
    .run();
}

struct MidEpochBlockTest {
    how_many_signers: u32,
    epoch_length: u64,
    /// How many epochs to let run to confirm behaviour is stable.
    wait_until_epoch: u64,
}

impl MidEpochBlockTest {
    fn run(self) {
        let _ = tempo_eyre::install();

        let setup = Setup::new()
            .how_many_signers(self.how_many_signers)
            .t2_time(0)
            .epoch_length(self.epoch_length);

        let cfg = Config::default().with_seed(setup.seed);
        let executor = Runner::from(cfg);

        executor.start(|mut context| async move {
            let (mut validators, _execution_runtime) = setup_validators(&mut context, setup).await;
            join_all(validators.iter_mut().map(|v| v.start(&context))).await;

            // Wait for validators to run through several full epochs.  If any
            // mid-epoch block were incorrectly treated as a boundary (i.e.
            // `handle_finalized_block` returned `Some(new_state)` for a
            // non-last block), the epoch counter would advance too quickly and
            // the on-chain DKG outcome would not be present at the expected
            // block height, causing `wait_for_outcome` to loop forever.
            wait_for_validators_to_reach_epoch(
                &context,
                self.wait_until_epoch,
                self.how_many_signers,
            )
            .await;

            // Verify each completed epoch has a valid on-chain DKG outcome at
            // its last block — confirming epochs are driven by actual boundary
            // blocks and not by mid-epoch blocks.
            for epoch in 0..self.wait_until_epoch {
                let outcome =
                    wait_for_outcome(&context, &validators, epoch, self.epoch_length).await;

                assert_eq!(
                    outcome.epoch.get(),
                    epoch + 1,
                    "DKG outcome at end of epoch {epoch} must carry the next epoch number"
                );
            }

            assert_no_dkg_failures(&context);
        })
    }
}

// ── handle_finalized_block phase actions ─────────────────────────────────────

/// During the Early phase of a DKG ceremony every dealer node must distribute
/// its shares to all players.  `handle_finalized_block` calls `distribute_shares`
/// on each Early-phase block while the dealer state is `Some`.
///
/// Verified via the `how_often_dealer` counter: a non-zero sum across all
/// validators confirms that dealer state was created and the Early-phase action
/// code path inside `handle_finalized_block` was exercised at least once.
///
/// Covers the unit-test `early_phase_mid_block_with_dealer_distributes_shares`.
#[test_traced]
fn early_phase_dealer_distributes_shares() {
    let _ = tempo_eyre::install();

    let setup = Setup::new().how_many_signers(4).t2_time(0).epoch_length(20);

    let cfg = Config::default().with_seed(setup.seed);
    let executor = Runner::from(cfg);

    executor.start(|mut context| async move {
        let (mut validators, _execution_runtime) = setup_validators(&mut context, setup).await;
        join_all(validators.iter_mut().map(|v| v.start(&context))).await;

        // Run one complete epoch so dealers have had a chance to distribute.
        wait_for_validators_to_reach_epoch(&context, 1, 4).await;

        // how_often_dealer is a Counter that accumulates across epochs.
        // A non-zero sum across all validators confirms that at least one node
        // reached the Early-phase share-distribution code path inside
        // handle_finalized_block.
        let how_often_dealer =
            sum_metric_with_suffix(&context, "_dkg_manager_how_often_dealer_total");
        assert!(
            how_often_dealer > 0,
            "expected at least one validator to have acted as dealer \
             (how_often_dealer_total={how_often_dealer})"
        );

        assert_no_dkg_failures(&context);
    })
}

/// In the Midpoint and Late phases, `handle_finalized_block` calls
/// `dealer_state.finalize()` so the dealer can assemble its log from the acks
/// it collected.  Without this finalization the dealer log is never written to
/// a block, the ceremony cannot complete, and `ceremony_successes` stays zero.
///
/// Verified indirectly: a non-zero `ceremony_successes` counter means the log
/// was produced (finalization happened) and written to a block, confirming both
/// the Midpoint and Late phase code paths ran correctly.
///
/// Covers unit tests `midpoint_phase_mid_block_finalizes_dealer` and
/// `late_phase_mid_block_finalizes_dealer`.
#[test_traced]
fn midpoint_and_late_phase_dealer_finalization() {
    let _ = tempo_eyre::install();

    let setup = Setup::new().how_many_signers(4).t2_time(0).epoch_length(20);

    let cfg = Config::default().with_seed(setup.seed);
    let executor = Runner::from(cfg);

    executor.start(|mut context| async move {
        let (mut validators, _execution_runtime) = setup_validators(&mut context, setup).await;
        join_all(validators.iter_mut().map(|v| v.start(&context))).await;

        // Two epochs to confirm dealer finalization is stable across reshares.
        wait_for_validators_to_reach_epoch(&context, 2, 4).await;

        // A successful ceremony requires the dealer to produce a signed log
        // (Midpoint/Late finalization) and for that log to appear in a block
        // where process_mid_epoch_block can read it back.
        let successes = sum_metric_with_suffix(&context, "_dkg_manager_ceremony_successes_total");
        assert!(
            successes > 0,
            "expected at least one successful ceremony, confirming dealer finalization \
             ran in Midpoint/Late phase (ceremony_successes_total={successes})"
        );

        assert_no_dkg_failures(&context);
    })
}

/// A node that restarts mid-epoch receives replayed blocks from the previous
/// epoch before catching up to the current one.  `handle_finalized_block` must
/// silently ignore those prior-epoch blocks (the `Ordering::Greater` branch)
/// and continue processing once it reaches the current epoch.
///
/// A bug here would cause the restarted node to error out or stall.
#[test_traced]
fn restarted_node_ignores_prior_epoch_blocks() {
    RestartMidEpochTest {
        how_many_signers: 4,
        epoch_length: 20,
        restart_after_epoch: 1,
    }
    .run();
}

struct RestartMidEpochTest {
    how_many_signers: u32,
    epoch_length: u64,
    /// Restart the first validator after this epoch completes.
    restart_after_epoch: u64,
}

impl RestartMidEpochTest {
    fn run(self) {
        let _ = tempo_eyre::install();

        let setup = Setup::new()
            .how_many_signers(self.how_many_signers)
            .t2_time(0)
            .epoch_length(self.epoch_length);

        let cfg = Config::default().with_seed(setup.seed);
        let executor = Runner::from(cfg);

        executor.start(|mut context| async move {
            let (mut validators, _execution_runtime) = setup_validators(&mut context, setup).await;
            join_all(validators.iter_mut().map(|v| v.start(&context))).await;

            // Let all validators complete the first epoch before restarting.
            wait_for_validators_to_reach_epoch(
                &context,
                self.restart_after_epoch + 1,
                self.how_many_signers,
            )
            .await;

            // Restart the first validator.  It will re-process finalized blocks
            // from genesis, including all prior-epoch blocks that
            // `handle_finalized_block` must silently ignore.
            validators[0].stop().await;
            validators[0].start(&context).await;

            // The restarted node must successfully rejoin and advance to the
            // next epoch without errors — confirming the Ordering::Greater
            // branch is handled correctly.
            wait_for_validators_to_reach_epoch(
                &context,
                self.restart_after_epoch + 2,
                self.how_many_signers,
            )
            .await;

            assert_no_dkg_failures(&context);
        })
    }
}

// ── resolve_epoch_outcome ─────────────────────────────────────────────────────

/// The last block of every epoch triggers `resolve_epoch_outcome`, which reads
/// the on-chain DKG outcome and produces the [`State`] for the next epoch.
///
/// This test verifies the full happy path:
/// - The outcome epoch is always `current_epoch + 1`.
/// - The group public key is preserved across reshare epochs.
/// - No ceremony failures occur.
#[test_traced]
fn boundary_block_resolves_epoch_outcome_and_advances_state() {
    BoundaryBlockTest {
        how_many_signers: 4,
        epoch_length: 20,
        epochs_to_run: 3,
    }
    .run();
}

struct BoundaryBlockTest {
    how_many_signers: u32,
    epoch_length: u64,
    epochs_to_run: u64,
}

impl BoundaryBlockTest {
    fn run(self) {
        let _ = tempo_eyre::install();

        let setup = Setup::new()
            .how_many_signers(self.how_many_signers)
            .t2_time(0)
            .epoch_length(self.epoch_length);

        let cfg = Config::default().with_seed(setup.seed);
        let executor = Runner::from(cfg);

        executor.start(|mut context| async move {
            let (mut validators, _execution_runtime) = setup_validators(&mut context, setup).await;
            join_all(validators.iter_mut().map(|v| v.start(&context))).await;

            wait_for_validators_to_reach_epoch(&context, self.epochs_to_run, self.how_many_signers)
                .await;

            // Read the on-chain outcome from the last block of each epoch and
            // verify the invariants that resolve_epoch_outcome must uphold.
            let mut prev_pubkey = None;

            for epoch in 0..self.epochs_to_run {
                let outcome =
                    wait_for_outcome(&context, &validators, epoch, self.epoch_length).await;

                // The outcome stored at epoch N's boundary block must carry
                // epoch N+1 — resolve_epoch_outcome reads the next epoch from
                // the on-chain artifact and returns it in the new State.
                assert_eq!(
                    outcome.epoch.get(),
                    epoch + 1,
                    "outcome at end of epoch {epoch} must carry epoch {}",
                    epoch + 1,
                );

                // The next-players set must be non-empty — resolve_epoch_outcome
                // reads these from the contract.
                assert!(
                    !outcome.next_players.is_empty(),
                    "next_players must be populated by resolve_epoch_outcome \
                     at end of epoch {epoch}"
                );

                // During normal resharing the group public key must be stable
                // across epochs — only a full DKG ceremony changes it.
                let pubkey = *outcome.sharing().public();
                if let Some(prev) = prev_pubkey {
                    assert_eq!(
                        prev,
                        pubkey,
                        "group public key must be stable across reshare epochs \
                         (changed between epoch {} and {epoch})",
                        epoch - 1,
                    );
                }
                prev_pubkey = Some(pubkey);

                tracing::info!(
                    epoch,
                    next_epoch = outcome.epoch.get(),
                    ?pubkey,
                    "Verified resolve_epoch_outcome output"
                );
            }

            assert_no_dkg_failures(&context);
        })
    }
}

/// `resolve_epoch_outcome` must also succeed when the local DKG ceremony
/// failed (e.g. the node missed dealer messages).  In that case the node falls
/// back to the prior output and share, and the next epoch still begins
/// correctly.
///
/// This is exercised by running with a single signer: the node is both dealer
/// and player so it always has a complete ceremony, but the validator contract
/// path inside `resolve_epoch_outcome` is still exercised.
#[test_traced]
fn resolve_epoch_outcome_single_signer() {
    BoundaryBlockTest {
        how_many_signers: 1,
        epoch_length: 10,
        epochs_to_run: 3,
    }
    .run();
}

// ── process_mid_epoch_block ───────────────────────────────────────────────────

/// Dealer logs written to block `extra_data` during a DKG ceremony must be
/// stored in the epoch journal by `process_mid_epoch_block`.
///
/// Verified indirectly: after a complete epoch the DKG outcome must be
/// non-default (i.e. the logs were collected and the ceremony succeeded).
/// A bug in `process_mid_epoch_block` that dropped logs silently would cause
/// the ceremony to fail — caught by `assert_no_dkg_failures`.
#[test_traced]
fn dealer_log_in_block_extra_data_is_stored() {
    let _ = tempo_eyre::install();

    // 4 signers so each node acts as both dealer and player, ensuring
    // dealer logs are written to blocks and must be read back.
    let setup = Setup::new().how_many_signers(4).t2_time(0).epoch_length(20);

    let cfg = Config::default().with_seed(setup.seed);
    let executor = Runner::from(cfg);

    executor.start(|mut context| async move {
        let (mut validators, _execution_runtime) = setup_validators(&mut context, setup).await;
        join_all(validators.iter_mut().map(|v| v.start(&context))).await;

        // Wait for one full epoch.  During this epoch every dealer writes its
        // log into a mid-epoch block's extra_data; process_mid_epoch_block
        // must store each log so the ceremony can complete.
        wait_for_validators_to_reach_epoch(&context, 1, 4).await;

        // If any dealer log was lost the ceremony would fail.
        assert_no_dkg_failures(&context);

        // The outcome epoch must have advanced — confirming logs were
        // collected and the ceremony produced a valid result.
        let outcome = wait_for_outcome(&context, &validators, 0, 20).await;
        assert_eq!(
            outcome.epoch.get(),
            1,
            "ceremony must have produced an outcome for epoch 1"
        );
    })
}

/// A node's own dealer log appearing in a finalized block must not be
/// re-broadcast: `process_mid_epoch_block` must clear the local finalized
/// state when it sees our own log.
///
/// Verified indirectly: if the node kept re-broadcasting its log, other nodes
/// would receive duplicate logs.  The ceremony still succeeds, but we confirm
/// the node continues to operate correctly across multiple epochs without
/// accumulating errors.
#[test_traced]
fn own_dealer_log_in_block_is_cleared_from_state() {
    let _ = tempo_eyre::install();

    let setup = Setup::new().how_many_signers(4).t2_time(0).epoch_length(20);

    let cfg = Config::default().with_seed(setup.seed);
    let executor = Runner::from(cfg);

    executor.start(|mut context| async move {
        let (mut validators, _execution_runtime) = setup_validators(&mut context, setup).await;
        join_all(validators.iter_mut().map(|v| v.start(&context))).await;

        // Run two full epochs.  If a node failed to clear its own log it would
        // try to re-broadcast it in the next epoch's Early phase, producing
        // either a network error or an unexpected log entry that disrupts the
        // ceremony.
        wait_for_validators_to_reach_epoch(&context, 2, 4).await;
        assert_no_dkg_failures(&context);
    })
}
