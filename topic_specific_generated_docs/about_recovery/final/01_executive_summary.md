# 01 — Executive Summary

[← index](index.md) | [next: Architecture Overview →](02_architecture_overview.md)

---

## What "recovery" is

PostgreSQL's recovery subsystem is the **receive-and-replay side of
WAL**. While the WAL write path makes durable a record of every
change, the recovery subsystem reads those records back from a
non-volatile source and replays them onto data files until the
on-disk image is consistent.

It runs in three configurations that share **one driver**:

* **Crash recovery** — after an unclean shutdown. The driver consumes
  WAL from `pg_wal/`, replays from the last checkpoint's REDO point,
  stops at end of WAL, then writes a clean checkpoint. Brief.
* **Archive recovery / point-in-time recovery (PITR)** — restore a
  base backup, then drive the redo loop from `restore_command`
  (archive) and `pg_wal/` until a configured stop predicate
  (`recovery_target_*`) fires. Used for backups, snapshots, forensic
  recovery.
* **Hot standby (continuous recovery)** — replicates a primary by
  streaming WAL via `walreceiver` plus optional fall-back to archive.
  Replays forever; only stops on promotion or shutdown. Optionally
  serves read-only queries throughout.

The same code path covers all three. They differ only in *where* the
next WAL page comes from (`XLOG_FROM_PG_WAL`, `XLOG_FROM_ARCHIVE`,
`XLOG_FROM_STREAM` — selected by `WaitForWALToBecomeAvailable`) and
*when* the redo loop terminates.

## The redo loop is the central state machine

The Startup auxiliary process forked by the postmaster runs
`StartupXLOG` (`xlog.c:5384`), which delegates to:

1. `InitWalRecovery` — open `pg_control`, parse signal files, parse
   `backup_label` if present, allocate `XLogReader` and
   `XLogPrefetcher`, position at the REDO start point.
2. `PerformWalRecovery` — the redo loop. For every WAL record:
   pause-check → `recoveryStopsBefore` → `recoveryApplyDelay` →
   `ApplyWalRecord` (which dispatches to the resource manager's
   `rm_redo` callback) → `recoveryStopsAfter` → fetch next record.
3. `FinishWalRecovery` — capture end-of-WAL state.
4. End-of-recovery actions in `StartupXLOG` itself: 2PC restoration,
   `XLOG_END_OF_RECOVERY` or end-of-recovery checkpoint, timeline
   bump, `pg_control` flip to `DB_IN_PRODUCTION`,
   `PMSIGNAL_RECOVERY_COMPLETED`.

`ApplyWalRecord` (`xlogrecovery.c:1908`) is the per-record dispatcher:
TLI switch detection, KnownAssignedXids tracking, the
`xlogrecovery_redo` recovery-driver-private hook, and finally
`GetRmgr(record->xl_rmid).rm_redo(xlogreader)` — the sole bridge
from the recovery driver to the **22 redo callbacks** registered in
`rmgrlist.h`.

## The consistency point promise

`pg_control->minRecoveryPoint` is the LSN at which the on-disk
image becomes a consistent snapshot. Recovery establishes the
following invariants:

* On entry, `pg_control` records the previous run's `state` and
  `minRecoveryPoint`.
* While replaying, every page modification advances
  `minRecoveryPoint` (via `UpdateMinRecoveryPoint` from buffer
  flushes / restartpoints).
* `CheckRecoveryConsistency` flips `reachedConsistency` to true once
  `lastReplayedEndRecPtr >= minRecoveryPoint` and `backupEndPoint`
  has been cleared.
* After consistency, hot standby may open for read-only queries
  (`PMSIGNAL_BEGIN_HOT_STANDBY`).

For recovery from a base backup taken on a running primary,
`backup_label` overrides the redo start LSN with the backup-time
checkpoint, and `XLOG_BACKUP_END` (replayed near the end of the
backup window) marks the post-backup consistency. Until both
conditions are met, the on-disk image is **transactionally
inconsistent**.

## Strict apply versus read availability — the tension

Recovery has two competing goals on a hot standby:

* **Strict apply.** Every WAL record must be replayed in order
  before any query reads the data the record affects. The redo loop
  must not skip records or leak in-progress states.
* **Read availability.** A backend's snapshot may be stale relative
  to records currently being applied. If the redo loop applies a
  record that physically removes a row visible to that snapshot
  (e.g., heap pruning), the backend's read is wrong.

The resolution is the **recovery-conflict** machinery
(`standby.c`'s `Resolve*` family). For each kind of conflict
(`PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`, `_LOCK`, `_BUFFERPIN`,
`_DATABASE`, `_TABLESPACE`, `_LOGICALSLOT`, `_STARTUP_DEADLOCK`),
the startup process signals victim backends and waits up to
`max_standby_*_delay` before canceling them. The `Resolve*`
functions all share a single subroutine,
`ResolveRecoveryConflictWithVirtualXIDs`, that implements the
"signal then wait then cancel" loop.

For the per-conflict details (resolver, grace-period GUC, victim
selection, mitigation), see
[18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md).

## The four data flows

In a fully-running standby, four data flows operate simultaneously:

1. **Record fetch.** `walreceiver` writes received WAL into
   `pg_wal/`; the Startup process reads from disk via
   `XLogPageRead` → `WaitForWALToBecomeAvailable`. (Crash recovery
   uses only `XLOG_FROM_PG_WAL`; archive recovery adds
   `XLOG_FROM_ARCHIVE`.)
2. **Apply.** `PerformWalRecovery` runs the redo loop:
   `ReadRecord` → `ApplyWalRecord` → `rm_redo`.
3. **Conflict resolution.** Whenever `rm_redo` calls
   `ResolveRecoveryConflictWith*`, the Startup process sends
   `PROCSIG_RECOVERY_CONFLICT_*` to victims; their next CFI
   processes the signal.
4. **Restartpoint.** Whenever `xlog_redo` replays
   `XLOG_CHECKPOINT_*`, `RecoveryRestartPoint` posts a request;
   the Checkpointer process eventually runs `CreateRestartPoint`
   to flush buffers, advance `minRecoveryPoint`, and recycle
   `pg_wal/`.

See the diagrams in [02_architecture_overview.md](02_architecture_overview.md)
for visualizations.

## Where to go next

* **Trace the lifecycle.** [03_recovery_driver_and_lifecycle.md](03_recovery_driver_and_lifecycle.md)
* **Understand a specific GUC.** [appendix_guc_parameters.md](appendix_guc_parameters.md)
* **Understand a specific redo callback.** [17_redo_callback_catalog.md](17_redo_callback_catalog.md)
* **Understand a specific recovery conflict.** [18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md)
* **Look up a function signature.** [recovery_api_reference.md](recovery_api_reference.md)
