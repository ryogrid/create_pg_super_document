# Redo Callbacks: `xlog_redo` and `xact_redo`

These are the two most important non-data-page redo callbacks:
`xlog_redo` (RM_XLOG) replays the recovery-driver records, and
`xact_redo` (RM_XACT) replays transaction commit/abort/prepare
records.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `xlog_redo` — RM_XLOG_ID = 0

### Identity

* **rmgr id**: `RM_XLOG_ID = 0`
* **rmgr name**: `"XLOG"`
* **redo function**: `xlog_redo` at
  `src/backend/access/transam/xlog.c:8251`
* **header**: declared in `src/include/access/xlog.h`

### Handled records (info-byte families)

| Info byte | Constant | Purpose |
|-----------|----------|---------|
| `0x00` | `XLOG_CHECKPOINT_SHUTDOWN` | Shutdown checkpoint; full counter authority. |
| `0x10` | `XLOG_CHECKPOINT_ONLINE` | Online checkpoint; counters are minima. |
| `0x20` | `XLOG_NOOP` | No-op (placeholder). |
| `0x30` | `XLOG_NEXTOID` | Sets `TransamVariables->nextOid`. |
| `0x40` | `XLOG_SWITCH` | Force a WAL segment switch. |
| `0x50` | `XLOG_BACKUP_END` | End of base backup; clears `backupEndPoint`. |
| `0x60` | `XLOG_PARAMETER_CHANGE` | Replay of primary GUC change affecting standby. |
| `0x70` | `XLOG_RESTORE_POINT` | Named restore point; matched by `recoveryStopsAfter` for `recovery_target_name`. |
| `0x80` | `XLOG_FPW_CHANGE` | Toggle `full_page_writes`. |
| `0x90` | `XLOG_END_OF_RECOVERY` | TLI bump marker. |
| `0xA0` | `XLOG_FPI_FOR_HINT` | Hint-bit-only FPI (torn-page-safe write). |
| `0xB0` | `XLOG_FPI` | Forced FPI. |
| `0xD0` | `XLOG_OVERWRITE_CONTRECORD` | Skip a known-bad continuation record. |
| `0xE0` | `XLOG_CHECKPOINT_REDO` | Marks where REDO begins inside an online checkpoint. |

Payload structs:

* `CheckPoint` (`src/include/catalog/pg_control.h`)
* `xl_parameter_change`, `xl_restore_point`,
  `xl_overwrite_contrecord`, `xl_end_of_recovery`
  (`src/include/access/xlog.h`)

### State mutations

| Target | What happens |
|--------|--------------|
| `TransamVariables` | `nextXid`/`nextOid`/`oldestXid`/`oldestMulti` advanced |
| `ControlFile` | `checkPointCopy.nextXid`, `wal_level` etc. (PARAMETER_CHANGE) |
| `XLogCtl->ckptFullXid` | Updated on shutdown checkpoints |
| `XLogCtl->RedoRecPtr` | Updated at REDO boundary |
| `fullPageWrites` | Set/cleared on FPW_CHANGE |
| `backupEndPoint` | Cleared on BACKUP_END (after match check) |
| `XLogRecoveryCtl->lastReplayedReadRecPtr` | Updated by ApplyWalRecord (post-rmgr) |
| Postmaster state | `RecoveryRestartPoint` posted on CHECKPOINT records |

### Hot-standby behavior

* `XLOG_CHECKPOINT_SHUTDOWN` ⇒ on standby, calls
  `ProcArrayApplyRecoveryInfo` with empty `xl_running_xacts` (no
  in-flight xids).
* `XLOG_PARAMETER_CHANGE` ⇒ may force standby to ereport(FATAL) if
  `max_connections`, `max_worker_processes`, `max_wal_senders`,
  `max_prepared_xacts`, `max_locks_per_xact`, `wal_level`,
  `wal_log_hints`, or `track_commit_timestamp` *tightens* below
  what the standby needs for its open backends.
* `XLOG_RESTORE_POINT` ⇒ matched by `recoveryStopsAfter` when
  `recovery_target_name` matches.

### Idempotency / LSN-skip

* CHECKPOINT records: idempotent — replaying twice updates the
  same global state to the same values.
* FPI records (`XLOG_FPI`, `XLOG_FPI_FOR_HINT`): always restored
  unconditionally via `XLogReadBufferForRedo` / `BLK_RESTORED`.
* Other records mutate global counters; the LSN-skip optimization
  doesn't apply (no page LSN to compare).

### Crash safety

After `xlog_redo` of a checkpoint, the cluster has a known-good
counter state and a known REDO start. `RecoveryRestartPoint` is
called to ensure the durable on-disk state matches.

### Example

A `XLOG_CHECKPOINT_SHUTDOWN` record causes:

1. Update `TransamVariables->nextXid` to `checkPoint.nextXid`.
2. Update `oldestXid`, `oldestXidDB`, `oldestMulti`, etc.
3. Update `XLogCtl->RedoRecPtr` to `checkPoint.redo`.
4. On standby: `ProcArrayApplyRecoveryInfo({xcnt=0})` — empty,
   resets KnownAssignedXids if state was DISABLED/INITIALIZED.
5. `RecoveryRestartPoint(&checkPoint, record)` posts a
   restartpoint request to the checkpointer.

---

## `xact_redo` — RM_XACT_ID = 1

### Identity

* **rmgr id**: `RM_XACT_ID = 1`
* **rmgr name**: `"Transaction"`
* **redo function**: `xact_redo` at
  `src/backend/access/transam/xact.c:6301`
* **header**: declared in `src/include/access/xact.h`

### Handled records (info-byte families)

Mask: `XLOG_XACT_OPMASK = 0x70` (lowest three bits).

| Info | Helper | Purpose |
|------|--------|---------|
| `0x00` | `xact_redo_commit` | Replay COMMIT |
| `0x10` | `PrepareRedoAdd` | Replay PREPARE |
| `0x20` | `xact_redo_abort` | Replay ABORT |
| `0x30` | `xact_redo_commit + PrepareRedoRemove` | Replay COMMIT_PREPARED |
| `0x40` | `xact_redo_abort + PrepareRedoRemove` | Replay ABORT_PREPARED |
| `0x50` | `ProcArrayApplyXidAssignment` | XLOG_XACT_ASSIGNMENT |
| `0x60` | (no-op in redo) | XLOG_XACT_INVALIDATIONS |

Payload structs (in `src/include/access/xact.h`):

* `xl_xact_commit` (parsed via `ParseCommitRecord` into
  `xl_xact_parsed_commit`)
* `xl_xact_abort` (parsed via `ParseAbortRecord` into
  `xl_xact_parsed_abort`)
* `xl_xact_assignment`

### State mutations

| Target | Action |
|--------|--------|
| CLOG (pg_xact) | `TransactionIdCommitTree(xid, ...)` / `TransactionIdAbortTree` |
| pg_subtrans | `SubTransSetParent` for subxids |
| TwoPhaseState shmem | `PrepareRedoAdd` / `PrepareRedoRemove` |
| KnownAssignedXids | `ExpireTreeKnownAssignedTransactionIds` (commit/abort) |
| smgr | `DropRelationsAllBuffers` + `smgrdounlink` for filenodesToDelete |
| sinval queue | `ProcessCommittedInvalidationMessages` (COMMIT only) |
| CommitTs | `TransactionTreeSetCommitTsData` (when `track_commit_timestamp=on`) |

### Hot-standby behavior

* Commit/abort: **`ExpireTreeKnownAssignedTransactionIds`** removes
  the xid (and all subxids) from the standby's KnownAssignedXids
  ring.
* Commit: **`ProcessCommittedInvalidationMessages`** broadcasts the
  primary's sinval messages so standby backends see the catalog
  changes (e.g., DDL committed).
* COMMIT records carry the xact_time used by `recoveryApplyDelay`
  (recovery_min_apply_delay).
* `XLOG_XACT_ASSIGNMENT`: subxids overflowed the PGPROC subxid
  cache; `ProcArrayApplyXidAssignment` registers them in
  KnownAssignedXids so standby snapshots are correct.

### Idempotency / LSN-skip

* CLOG writes are idempotent (writing "committed" to a slot that's
  already "committed" is a no-op).
* No page-LSN check (CLOG/SLRU has its own LSN tracking).
* Inval message broadcast is idempotent (standby backends process
  duplicates harmlessly).

### Crash safety

* The CLOG slot is written *after* the in-memory state is
  consistent, but the CLOG SLRU's own checkpointing makes the slot
  durable independently of the WAL replay.
* For prepared transactions, the `pg_twophase/<XID>` file is
  managed in lockstep: PREPARE writes it, COMMIT_PREPARED removes
  it (via `RemoveTwoPhaseFile`).

### Example

A `XLOG_XACT_COMMIT` record for xid 12345 with subxids
`{12346, 12347}` and one filenode-to-delete `<rel>`:

1. `TransactionIdCommitTree(12345, {12346, 12347}, ...)` writes
   "committed" to CLOG.
2. `ProcessCommittedInvalidationMessages(invals, nmsgs, ...)` —
   broadcasts inval messages.
3. On standby:
   `ExpireTreeKnownAssignedTransactionIds(12345, {12346, 12347})`.
4. `DropRelationsAllBuffers` + `smgrdounlink(<rel>, false)` —
   removes the dropped relation's data.
5. `recoveryApplyDelay` may have already waited based on the
   record's `xact_time` before this point.

---

## Source references

* `src/backend/access/transam/xlog.c:8251` — `xlog_redo`
* `src/backend/access/transam/xact.c:6301` — `xact_redo`
* `src/include/access/xlog.h` — `XLOG_*` info constants
* `src/include/access/xact.h` — `xl_xact_*`, `XLOG_XACT_*` constants
