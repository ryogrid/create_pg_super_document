# 20 — Deep Dives

[← Recovery Target Catalog](19_recovery_target_catalog.md) | [index](index.md) | [next: Symbol Index →](appendix_symbol_index.md)

---

This chapter discusses cross-cutting concerns that don't fit
cleanly into a single component module. Each section is
self-contained.

## Table of contents

1. [The three-variants-one-driver design](#1-the-three-variants-one-driver-design)
2. [`XLOG_FROM_*` source switching](#2-xlog_from_-source-switching)
3. [`backup_label` crash safety and `minRecoveryPoint`](#3-backup_label-crash-safety-and-minrecoverypoint)
4. [Recovery prefetch effectiveness](#4-recovery-prefetch-effectiveness)
5. [The `recovery.conf` → signal-file migration (PG12+)](#5-the-recoveryconf--signal-file-migration-pg12)
6. [`RecoveryInProgress()` lock-free fast path](#6-recoveryinprogress-lock-free-fast-path)
7. [Hot-standby snapshot construction from `RUNNING_XACTS`](#7-hot-standby-snapshot-construction-from-running_xacts)
8. [`KnownAssignedXids` ring buffer mechanics](#8-knownassignedxids-ring-buffer-mechanics)
9. [Recovery conflicts target VirtualXIDs](#9-recovery-conflicts-target-virtualxids)
10. [`max_standby_streaming_delay` vs `max_standby_archive_delay`](#10-max_standby_streaming_delay-vs-max_standby_archive_delay)
11. [AccessExclusiveLock on the standby](#11-accessexclusivelock-on-the-standby)
12. [2PC recovery: full vs standby variant](#12-2pc-recovery-full-vs-standby-variant)
13. [Restartpoint vs checkpoint](#13-restartpoint-vs-checkpoint)
14. [End-of-recovery WAL on the new timeline](#14-end-of-recovery-wal-on-the-new-timeline)
15. [Promotion race against shutdown](#15-promotion-race-against-shutdown)
16. [Custom rmgrs (Neon, Aurora, Citus)](#16-custom-rmgrs-neon-aurora-citus)
17. [`recovery_min_apply_delay` semantics](#17-recovery_min_apply_delay-semantics)
18. [`pg_wal_replay_pause` / `resume` during PITR](#18-pg_wal_replay_pause--resume-during-pitr)

---

## 1. The three-variants-one-driver design

Crash recovery, archive recovery, and hot-standby look very
different from outside the cluster — different restart files, different
config, different lifecycle. Internally they share **one driver**:
`StartupXLOG → InitWalRecovery → PerformWalRecovery → FinishWalRecovery`.

The variants diverge at exactly two points:

1. **`InitWalRecovery` parameter setting.** Based on the
   combination of `pg_control->state`, `recovery.signal`,
   `standby.signal`, and `backup_label`, sets globals
   `ArchiveRecoveryRequested`, `StandbyMode`, `InRecovery`,
   `LocalMinRecoveryPoint`.
2. **`WaitForWALToBecomeAvailable` source-selection logic.** The
   only state-machine that distinguishes the variants:
   crash uses only `XLOG_FROM_PG_WAL`; archive adds
   `XLOG_FROM_ARCHIVE`; standby adds both archive and
   `XLOG_FROM_STREAM`.

Everything else is identical. `PerformWalRecovery`'s loop body, the
22 redo callbacks, the conflict-resolution machinery, the
restartpoint flow — none of it knows or cares which variant is
running.

Why is this possible? Because the redo loop's contract is purely
*"give me records in order; I will mutate disk state and call you
back to fetch more"*. The variants only affect *what happens when
records run out*: in crash recovery that's "we're done"; in
archive that's "we're done unless `recovery_target_*` says we
should fail because we didn't reach the target"; in standby that's
"wait for more".

**Practical consequence.** Bug fixes in `ApplyWalRecord` apply to
all three. Performance improvements in the redo loop benefit all
three. Adding a new redo callback (a new rmgr) requires no
variant-specific work.

---

## 2. `XLOG_FROM_*` source switching

`WaitForWALToBecomeAvailable` (`xlogrecovery.c:3542`) is the
state machine that picks among three WAL sources:

```c
#define XLOG_FROM_ANY      0  /* pseudo: try any */
#define XLOG_FROM_ARCHIVE  1  /* restore_command */
#define XLOG_FROM_PG_WAL   2  /* local pg_wal/ */
#define XLOG_FROM_STREAM   3  /* walreceiver */
```

Two file-static variables track current and previous source:
`currentSource` and `readSource`. The state machine is
*precedence-based*, not strictly transitional: in standby mode the
priority is `pg_wal → archive → stream`, but the machine retries
each one with backoff.

A subtle interaction: the **switch from crash to archive** does not
go through `WaitForWALToBecomeAvailable` — it happens in
`ReadRecord` (`xlogrecovery.c:3131`). When `pg_wal/` is exhausted
during what was supposed to be crash recovery but signal files
say "you have an archive too", `ReadRecord` calls
`SwitchIntoArchiveRecovery`, sets `InArchiveRecovery = true`, and
restarts the source loop. This is the path documented in
`pg_control_state_machine.mermaid` as `DB_IN_CRASH_RECOVERY →
DB_IN_ARCHIVE_RECOVERY`.

After the switch, `currentSource = XLOG_FROM_ANY` so the next
iteration re-prioritizes (archive first now).

---

## 3. `backup_label` crash safety and `minRecoveryPoint`

A base backup taken on a running primary may have copied pages
mid-write — a buffer flush could have happened during the copy.
The `backup_label` file (`pg_basebackup` writes it) records:

* `START WAL LOCATION` — the LSN of the checkpoint that defines the
  backup's redo start.
* `BACKUP METHOD` — `streamed` or `pg_backup_start`.

`read_backup_label` (`xlogrecovery.c:1208`) overrides the redo
start LSN with the file's `START WAL LOCATION`. If `BACKUP METHOD ==
streamed`, it sets `backupEndRequired = true`, and recovery is
*unsafe* until `XLOG_BACKUP_END` is replayed (which clears
`backupEndPoint`).

The two-LSN dance is:

| Variable | Set by | Cleared by |
|----------|--------|-----------|
| `backupStartPoint` | `read_backup_label` | `CheckRecoveryConsistency` once consistency is reached |
| `backupEndPoint` | `xlog_redo` of `XLOG_BACKUP_END` | (not cleared; just compared) |
| `backupEndRequired` | `read_backup_label` (true if streamed) | n/a |
| `minRecoveryPoint` | every page-flushing redo step | n/a |

`reachedConsistency` flips true when:

```
lastReplayedEndRecPtr >= minRecoveryPoint
  AND (!backupEndRequired || backupEndPoint <= lastReplayedEndRecPtr)
```

Until both conditions hold, the on-disk image is **not a consistent
snapshot**. Hot standby refuses queries; reading `pg_control` would
show `state=DB_IN_ARCHIVE_RECOVERY` with `minRecoveryPoint > 0`.

Crash safety case: if recovery itself crashes mid-replay, the next
recovery starts from the *new* `pg_control->checkPoint` (which has
been advanced by restartpoints) — but `minRecoveryPoint` is also
preserved, so the consistency point still applies.

---

## 4. Recovery prefetch effectiveness

`recovery_prefetch` (`off / on / try`, default `try`) wraps the
WAL reader with `XLogPrefetcher`. The prefetcher walks records
ahead of the redo loop's read position, extracts referenced blocks,
and issues `PrefetchSharedBuffer` so the redo callback's
`XLogReadBufferForRedo` finds a warm buffer.

The window is bounded by `maintenance_io_concurrency` (default 10
on Linux). A reasonable approximation:

```
prefetch_window_lsn ≈ maintenance_io_concurrency × average_record_size
```

For a typical OLTP record of ~200 bytes, that's about 2 KB ahead —
not much. But for pages-per-record-heavy workloads (vacuum), the
benefit is much higher because the prefetcher pre-loads pages that
would otherwise stall the redo loop on `read(2)`.

Effectiveness is best measured via `pg_stat_recovery_prefetch`:

```sql
SELECT * FROM pg_stat_recovery_prefetch;
-- prefetch | hit | skip_init | skip_new | skip_fpw | skip_rep | wal_distance | block_distance | io_depth
```

* `prefetch` — number of `posix_fadvise(WILLNEED)` calls.
* `hit` — number of records the redo loop found warm.
* `skip_*` — reasons records were not prefetched (FPI, page
  initialization, recently-replayed, etc.).

A high `hit / prefetch` ratio means prefetch is paying off; a high
`skip_fpw` means the workload is heavy on full-page images (which
are self-contained and don't need prefetch).

---

## 5. The `recovery.conf` → signal-file migration (PG12+)

PostgreSQL ≤ 11 used a single `recovery.conf` file containing all
recovery-related GUCs (`restore_command`, `recovery_target_*`,
`primary_conninfo`, `primary_slot_name`,
`recovery_min_apply_delay`, …) plus the implicit "I want to
recover" signal (the file's *existence*).

PG ≥ 12 split this into two parts:

* **GUCs** moved into `postgresql.conf` /
  `postgresql.auto.conf` (with `PGC_POSTMASTER` for most,
  `PGC_SIGHUP` for `recovery_min_apply_delay`,
  `wal_receiver_status_interval`, etc.).
* **Recovery-requested signal** moved to `recovery.signal` (PITR)
  and `standby.signal` (replication standby).

Compatibility notes:

* If `recovery.conf` is found in `$PGDATA`, the server **refuses to
  start** with a hint to migrate parameters into `postgresql.conf`.
  This is intentional: silently ignoring the file would leave the
  operator thinking their old config is still in effect.
* `pg_basebackup -R` writes `standby.signal` plus
  `postgresql.auto.conf` containing `primary_conninfo` and (if a
  slot is requested) `primary_slot_name`.
* `recovery_target_*` are now `PGC_POSTMASTER` GUCs; they cannot be
  changed without restart, just like before — but now they can be
  set in `postgresql.conf` for review.

---

## 6. `RecoveryInProgress()` lock-free fast path

`RecoveryInProgress()` is consulted by every backend's write path
*at least once per transaction, often per statement*. It must be
fast. The implementation uses:

```c
bool
RecoveryInProgress(void)
{
    /* (1) Process-local cache: monotonic.
     * Once we have seen "not in recovery", we never see "in
     * recovery" again, because the cluster cannot re-enter
     * recovery without restarting the process. */
    if (!LocalRecoveryInProgress)
        return false;

    /* (2) Shared atomic check */
    bool localState = (XLogCtl->SharedRecoveryState != RECOVERY_STATE_DONE);
    if (!localState) {
        /* (3) Memory barrier; re-read globals the recovery driver
         * published before the flip. */
        pg_memory_barrier();
        InitXLOGAccess();
        LocalRecoveryInProgress = false;
    }
    return LocalRecoveryInProgress;
}
```

The contract: `XLogCtl->SharedRecoveryState` is the *atomic*
publishing channel. When the recovery driver flips it to
`RECOVERY_STATE_DONE`, all backends will eventually see the change
on their next call (with a memory-barrier-safe read).

The monotonicity (step 1) is what makes the per-process cache
sound. It also means a backend that has cached "not in recovery"
will *never* re-check the shared state, paying zero cost on
subsequent calls. This is critical for the millions-per-second
calls in a busy cluster.

---

## 7. Hot-standby snapshot construction from `RUNNING_XACTS`

A standby backend's `GetSnapshotData` cannot look at the **primary's**
procarray, so it must reconstruct the equivalent from WAL records.
The mechanism:

1. **Primary** periodically calls `LogStandbySnapshot` (from the
   bgwriter and at checkpoints), emitting an `XLOG_RUNNING_XACTS`
   record:

   ```c
   typedef struct xl_running_xacts {
       int            xcnt;
       int            subxcnt;
       bool           subxid_overflow;
       TransactionId  nextXid;
       TransactionId  oldestRunningXid;
       TransactionId  latestCompletedXid;
       TransactionId  xids[FLEXIBLE_ARRAY_MEMBER];
   } xl_running_xacts;
   ```

2. **Standby** replays via `standby_redo` (RM_STANDBY) ⇒
   `ProcArrayApplyRecoveryInfo(running)`:

   * Reset `KnownAssignedXids` ring.
   * Add every xid in `running->xids[]`.
   * Update `latestCompletedXid` for snapshot xmax.
   * Update `nextXid` if the record's is higher.
   * Transition `standbyState` to `SNAPSHOT_READY` if not
     overflowed.

3. **Subsequent records** maintain the ring incrementally:

   * Any record with `xl_xid != InvalidTransactionId` ⇒
     `KnownAssignedXidsAdd(xid)` from `RecordKnownAssignedTransactionIds`.
   * `XLOG_XACT_COMMIT` / `_ABORT` ⇒
     `ExpireTreeKnownAssignedTransactionIds(xid, subxids)`.
   * `XLOG_XACT_ASSIGNMENT` ⇒ `ProcArrayApplyXidAssignment`.

4. **Standby `GetSnapshotData`** walks the ring (sorted), filling
   `snapshot->xip[]` with running xids, and returns a snapshot
   semantically identical to one taken on the primary at the same
   LSN.

The "overflowed" case (`subxid_overflow=true`) is critical. If a
primary transaction has more than `PGPROC_MAX_CACHED_SUBXIDS`
(default 64) subtransactions, the record cannot list them all —
the standby can't know whether a given xid is a subxid of a
running transaction. In that case, `standbyState` remains
`SNAPSHOT_PENDING`, and queries fail with `database system is in
recovery; cannot accept connections` until a non-overflowed record
arrives.

This is why bulk loaders that use savepoints heavily can delay
hot-standby availability after restart.

---

## 8. `KnownAssignedXids` ring buffer mechanics

`KnownAssignedXids` lives in shared memory, protected by
`ProcArrayLock`. It's a **sorted** ring, not a hash, for two
reasons:

* `GetSnapshotData` must walk it linearly to fill `snapshot->xip[]`;
  sorted order makes the binary-search prune trivial.
* Most operations are at the head (new xids assigned via
  `KnownAssignedXidsAdd`) or scattered removes (commits/aborts).

Key helpers (in `procarray.c`):

| Helper | Purpose |
|--------|---------|
| `KnownAssignedXidsAdd` | Append a new xid (sorted insert at head). |
| `KnownAssignedXidsRemove` | Mark an xid `valid=false` (lazy delete). |
| `KnownAssignedXidsRemovePreceding` | Compress out everything < oldestRunningXid. |
| `KnownAssignedXidsCompress` | Squeeze out `valid=false` slots. |
| `KnownAssignedXidsSearch` | Binary search. |
| `KnownAssignedXidsGetOldestXmin` | Used by snapshot xmin computation. |
| `KnownAssignedXidsGetAndSetXmin` | Atomic snapshot-build helper. |

The ring is a fixed-size array sized to
`TOTAL_MAX_CACHED_SUBXIDS`; if it ever fills up, that's a hard
error — the standby cannot represent the snapshot.

Lazy deletion + periodic compression is the trade-off: writers
(commits/aborts) only mark `valid=false` to keep individual
operations O(log n); the compressor runs occasionally to keep the
ring dense for readers.

---

## 9. Recovery conflicts target VirtualXIDs

A standby backend running a read-only query has **no transaction
ID**. So the recovery-conflict machinery cannot use XIDs to
identify victims. It uses **VirtualTransactionId** =
`(backendId, localTransactionId)`:

```c
typedef struct VirtualTransactionId
{
    int                BackendId;          /* index into ProcArray */
    LocalTransactionId LocalTransactionId; /* per-backend counter */
} VirtualTransactionId;
```

`GetConflictingVirtualXIDs(snapshotConflictHorizon, dbOid)` walks
the procarray and returns every VXID whose snapshot's `xmin` is
older than `snapshotConflictHorizon` (the per-record horizon
extracted from the WAL record).

`SendProcSignal(vxid_to_pid(vxid), reason, ...)` delivers the
conflict signal. The standby backend's signal handler
(`HandleRecoveryConflictInterrupt`) sets a bit in
`RecoveryConflictPendingReasons[reason]`. The next
`CHECK_FOR_INTERRUPTS()` calls `ProcessRecoveryConflictInterrupt`
which dispatches: `ERROR` (snapshot/lock/tablespace), `FATAL`
(database/startup-deadlock), or release-pin (bufferpin idle).

The `(backendId, localXid)` pair is stable across the backend's
process lifetime — even when the backend has no XID assigned, its
VXID uniquely identifies its current "transaction context".

---

## 10. `max_standby_streaming_delay` vs `max_standby_archive_delay`

Both default to 30 seconds. They control how long the startup
process waits for backends to release before canceling them on a
recovery conflict. The choice between them is made by
`WaitExceedsMaxStandbyDelay` based on `XLogReceiptTime`:

* **Streaming**: `XLogReceiptTime` is set when walreceiver
  receives bytes. A streaming record has receipt time near "now",
  so `max_standby_streaming_delay` applies.
* **Archive**: `XLogReceiptTime` is set when a WAL segment file is
  *opened* during archive replay (via `RestoreArchivedFile`).
  Typically far in the past. So `max_standby_archive_delay`
  applies.

The default of 30 seconds for both is a compromise. Common
adjustments:

* **Standby is critical for queries, primary can wait**: set both
  to a high value or `-1` (= wait forever).
* **Standby must catch up at all cost**: set both to `0` (= cancel
  immediately on conflict).
* **Archive recovery from disk, no streaming**: set
  `max_standby_archive_delay = -1` (let recovery wait), keep
  streaming delay short.

---

## 11. AccessExclusiveLock on the standby

A primary's `AccessExclusiveLock` (e.g., from `ALTER TABLE`,
`CLUSTER`, `VACUUM FULL`) must be respected by the standby — but
the lock-holding transaction doesn't exist on the standby. The
mechanism:

1. **Primary** calls `LogAccessExclusiveLocks` from
   `LockAcquireExtended` ⇒ `XLOG_STANDBY_LOCK` record.
2. **Standby** `standby_redo` for `XLOG_STANDBY_LOCK` calls
   `StandbyAcquireAccessExclusiveLock(xid, db, rel)` for each lock,
   registering a **virtual lock** on behalf of the primary's xid
   using the recovery transaction-environment vxid.
3. **Standby backends** acquiring shared locks block against this
   virtual lock just like a regular lock.
4. **Primary commits** ⇒ `xact_redo_commit` calls
   `StandbyReleaseLockTree(xid)` which releases the virtual lock.
5. **End of recovery** ⇒ `StandbyReleaseAllLocks` clears any
   leftover virtual locks (defensive cleanup).

The cost: every primary-side `AccessExclusiveLock` writes a WAL
record. This is the price of correct standby visibility.

The benefit: standby backends see the same lock-blocking behavior
they'd see on the primary, modulo `max_standby_*_delay`.

---

## 12. 2PC recovery: full vs standby variant

PostgreSQL keeps prepared-transaction state in `pg_twophase/<XID>`
files (binary). Two flavors of recovery rebuild the in-memory
GXACT table:

* **`RecoverPreparedTransactions`** — called from `StartupXLOG`
  *after* redo finishes on a non-standby cluster. For each
  prepared xact still in shmem, re-read `pg_twophase/<XID>`,
  re-acquire heavyweight locks, restore subxact state, mark
  recovered. After this, normal `COMMIT PREPARED` and
  `ROLLBACK PREPARED` work.
* **`StandbyRecoverPreparedTransactions`** — called from
  `xact_redo` (PrepareRedoAdd path) when a prepared xact is
  encountered during replay. **Skips the lock-acquire step**, because
  locks come from `XLOG_STANDBY_LOCK` records via `standby_redo`,
  not from the 2PC file. Re-acquiring would double-lock.

The split is necessary because:

* On a non-standby crash recovery, no walreceiver has been delivering
  `XLOG_STANDBY_LOCK` records — the locks must be reconstructed from
  the 2PC file directly.
* On a standby, the primary has been (or will be) emitting
  `XLOG_STANDBY_LOCK` records during normal operation. The standby
  trusts those records as the lock source of truth.

---

## 13. Restartpoint vs checkpoint

| Aspect | Checkpoint (primary) | Restartpoint (standby) |
|--------|---------------------|------------------------|
| Where | Primary; runs on Checkpointer | Standby/recovering; runs on Checkpointer |
| Writes WAL | Yes (`XLOG_CHECKPOINT_*`) | **No** (cluster cannot write WAL) |
| Flushes buffers | Yes | Yes |
| Flushes SLRUs | Yes | Yes |
| Advances `minRecoveryPoint` | n/a | Yes |
| Updates `pg_control` | Yes (`checkPoint`) | Yes (`minRecoveryPoint`) |
| Recycles `pg_wal/` | Yes | Yes |
| Triggers `archive_cleanup_command` | No | Yes |

Both share `CheckPointGuts` for the actual flush work
(`CheckPointBuffers`, `CheckPointCLOG`, `CheckPointMultiXact`,
`CheckPointTwoPhase`, etc.). The dispatch is in
`CreateCheckPoint` (primary) and `CreateRestartPoint` (recovery).

The terminology is precise: a "standby checkpoint" is a misnomer
— the standby cannot write a `XLOG_CHECKPOINT_*` WAL record. Use
"restartpoint" instead.

The **trigger** is the same as on a primary: a checkpoint record
appears in the WAL (replayed via `xlog_redo`), and
`RecoveryRestartPoint` posts a request flag in
`XLogCtl->lastCheckPointIsRequired`. The Checkpointer's main loop
sees the flag and runs `CreateRestartPoint`. This is identical to
how `RequestCheckpoint` works on a primary.

---

## 14. End-of-recovery WAL on the new timeline

When recovery completes (on a standby that's been promoted, or on
an archive recovery that hit a target), the cluster must signal
the timeline change to anyone reading the WAL. Two mechanisms:

* **`XLOG_END_OF_RECOVERY`** (RM_XLOG, info 0x90). Carries
  `xl_end_of_recovery { ThisTimeLineID, PrevTimeLineID, end_time,
  fullPageWrites }`. Logged when the timeline is bumped.
  Downstream replicas detect the TLI switch in `ApplyWalRecord`.
* **End-of-recovery checkpoint** —
  `CreateCheckPoint(CHECKPOINT_END_OF_RECOVERY |
  CHECKPOINT_IMMEDIATE)`. Bumps `pg_control->checkPoint` and
  flushes everything. On a `DB_SHUTDOWNED` clean restart (no
  recovery actually run), this is a no-op equivalent.

The timeline ID is allocated as `findNewestTimeLine() + 1` (so a
gap is possible if other history files exist). The new history
file is written by `writeTimeLineHistory(newTLI, oldTLI,
switchpoint, reason)` — append-only across history files, so
parent ↔ child relationships are preserved.

`RemoveNonParentXlogFiles` is called to delete WAL segments on
the old TLI that are *after* the switchpoint (those segments are
stale on the new TLI).

---

## 15. Promotion race against shutdown

The postmaster can receive both `PMSIGNAL_PROMOTE` and SIGTERM
concurrently. The PMState machine handles this:

* **PMSIGNAL_PROMOTE during `PM_HOT_STANDBY`**: postmaster writes
  the `promote` signal file (so a Startup-process restart still
  sees the request) and signals the Startup process. The Startup
  process polls `CheckForStandbyTrigger` from inside the redo
  loop; on true, breaks out into `FinishWalRecovery`.
* **SIGTERM during promotion**: the postmaster has a
  `processing_promote` flag (and similar). If a SIGTERM arrives
  during the in-flight promotion, the postmaster **finishes** the
  promotion before processing the shutdown. The new TLI is
  written, `pg_control` is updated to `DB_IN_PRODUCTION`, then
  shutdown begins.
* **Promotion arriving during shutdown**: ignored. The cluster is
  already going down.

The race window is small (the timeline bump is ~1ms of work after
the redo loop ends), but it must be deterministic.

---

## 16. Custom rmgrs (Neon, Aurora, Citus)

Built-in rmgr IDs are 0..21 (defined in `rmgrlist.h`). IDs
128..255 are reserved for **custom rmgrs** registered via
`RegisterCustomRmgr` from an extension's `_PG_init`:

```c
void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr);
```

Extensions provide:

* `rm_redo` — per-record apply (mandatory).
* `rm_desc` / `rm_identify` — for `pg_waldump`.
* `rm_decode` — for logical replication consumers.
* `rm_startup` / `rm_cleanup` — optional one-shot hooks.

In practice this is used by:

* **Neon** — the storage offload. Custom records describe page
  modifications without writing the modified pages locally; the
  pageserver consumes them out-of-band.
* **AWS Aurora-style** — similar ideas: storage-side replay rather
  than node-side.
* **Citus** — for distributed coordination state changes.

Implementation guidance: a custom rmgr's redo callback must follow
the same idempotency rules as built-ins (LSN-skip via
`XLogReadBufferForRedo`, no shared state that can't be
reconstructed). `rm_decode` is required if the extension wants its
records consumable by logical decoding.

---

## 17. `recovery_min_apply_delay` semantics

The `recovery_min_apply_delay` GUC delays application of COMMIT
records by a wall-clock interval. The delay is measured from the
record's **`xact_time`** (commit time on the primary), not from
the standby's reception time. The reasoning:

* Two standbys with the same delay value reach the same
  application LSN at the same wall-clock time (modulo clock skew).
* The standby is exactly `recovery_min_apply_delay` behind the
  primary in transaction order, regardless of network jitter.
* The check is implemented in `recoveryApplyDelay`
  (`xlogrecovery.c:2982`):

  ```c
  delayUntil = TimestampTzPlusMilliseconds(xact_time,
                                           recovery_min_apply_delay);
  while (delayUntil > now) {
      WaitLatch(...);
      HandleStartupProcInterrupts();
      if (CheckForStandbyTrigger()) break;  /* promote interrupts */
      now = GetCurrentTimestamp();
  }
  ```

Aborts are not delayed (no MVCC effect). Records other than
`XLOG_XACT_COMMIT` and `XLOG_XACT_COMMIT_PREPARED` are applied
immediately.

A common operational pattern: keep one standby at
`recovery_min_apply_delay = '1h'` as protection against accidental
DROP TABLE on the primary. An operator notices within an hour and
promotes the delayed standby before it replays the COMMIT.

---

## 18. `pg_wal_replay_pause` / `resume` during PITR

A PITR can be **paused** with `pg_wal_replay_pause()`, inspected
with regular SELECT queries (since hot standby is enabled by
default during recovery), and either:

* **Resumed** with `pg_wal_replay_resume()` — recovery continues
  from the same record.
* **Promoted** with `pg_promote()` — recovery ends here, on a
  new timeline.

The pause is implemented via a condition variable
(`XLogRecoveryCtl->recoveryNotPausedCV`) so the resume signal
wakes the Startup process within milliseconds.

The three-state machine is critical:

| State | Meaning | Set by |
|-------|---------|--------|
| `RECOVERY_NOT_PAUSED` | Recovery is running | `pg_wal_replay_resume`, initial state |
| `RECOVERY_PAUSE_REQUESTED` | Backend asked for pause | `pg_wal_replay_pause` |
| `RECOVERY_PAUSED` | Startup process is actually paused | Startup process inside `recoveryPausesHere` |

Backends can call `pg_get_wal_replay_pause_state()` to know whether
the pause has been confirmed. This matters for orchestration: a
script may want to wait for `RECOVERY_PAUSED` before connecting
psql for inspection.

A subtle interaction: after a `recovery_target_action = pause` stop,
the redo loop is in `recoveryPausesHere(true)` — calling
`pg_wal_replay_resume` from a backend lets the loop fall through
to `RECOVERY_TARGET_ACTION_PROMOTE` (note the deliberate fall-through
in the action switch). So **resume after a target-pause = promote**.
This is documented in [07_recovery_target_system.md](07_recovery_target_system.md)
and the
[recovery_target_action](19_recovery_target_catalog.md#8-recovery_target_action)
catalog entry.
