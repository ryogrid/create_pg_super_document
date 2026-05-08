# 17 — Redo Callback Catalog

[← Hooks and Extensibility](16_hooks_and_extensibility.md) | [index](index.md) | [next: Recovery Conflict Catalog →](18_recovery_conflict_catalog.md)

---

This chapter is a per-callback reference for the **22 built-in redo
callbacks** registered in `src/include/access/rmgrlist.h`. Each entry
follows the standardized template:

* **Identity** — rmgr id, name, redo function symbol + source location.
* **Handled records** — the info-byte families dispatched to per-record
  helpers.
* **State mutations** — what shared/durable state changes.
* **Hot-standby behavior** — recovery conflicts, KnownAssignedXids
  side effects, etc.
* **Idempotency / LSN-skip** — whether re-applying is safe.
* **Crash safety** — what becomes durable after replay.
* **Example record** — at least one concrete payload + replay step.

For the dispatch table itself (`RmgrTable`, `GetRmgr`,
`RmgrStartup`, `RmgrCleanup`), see
[14_rmgr_dispatch.md](14_rmgr_dispatch.md). For the buffer-helper
contract used by every page-modifying redo callback, see
[15_recovery_buffer_helpers.md](15_recovery_buffer_helpers.md).

For a one-line-per-callback overview, see
[appendix_redo_callback_quick_reference.md](appendix_redo_callback_quick_reference.md).

## Catalog overview

| rmid | Name | Redo fn | Section |
|-----:|------|---------|---------|
| 0 | XLOG | `xlog_redo` | [§1](#1-xlog_redo--rm_xlog_id--0) |
| 1 | Transaction | `xact_redo` | [§2](#2-xact_redo--rm_xact_id--1) |
| 2 | Storage | `smgr_redo` | [§3](#3-smgr_redo--rm_smgr_id--2) |
| 3 | CLOG | `clog_redo` | [§4](#4-clog_redo--rm_clog_id--3) |
| 4 | Database | `dbase_redo` | [§5](#5-dbase_redo--rm_dbase_id--4) |
| 5 | Tablespace | `tblspc_redo` | [§6](#6-tblspc_redo--rm_tblspc_id--5) |
| 6 | MultiXact | `multixact_redo` | [§7](#7-multixact_redo--rm_multixact_id--6) |
| 7 | RelMap | `relmap_redo` | [§8](#8-relmap_redo--rm_relmap_id--7) |
| 8 | Standby | `standby_redo` | [§9](#9-standby_redo--rm_standby_id--8) |
| 9 | Heap2 | `heap2_redo` | [§10](#10-heap2_redo--rm_heap2_id--9) |
| 10 | Heap | `heap_redo` | [§11](#11-heap_redo--rm_heap_id--10) |
| 11 | Btree | `btree_redo` | [§12](#12-btree_redo--rm_btree_id--11) |
| 12 | Hash | `hash_redo` | [§13](#13-hash_redo--rm_hash_id--12) |
| 13 | Gin | `gin_redo` | [§14](#14-gin_redo--rm_gin_id--13) |
| 14 | Gist | `gist_redo` | [§15](#15-gist_redo--rm_gist_id--14) |
| 15 | Sequence | `seq_redo` | [§16](#16-seq_redo--rm_seq_id--15) |
| 16 | SPGist | `spg_redo` | [§17](#17-spg_redo--rm_spgist_id--16) |
| 17 | BRIN | `brin_redo` | [§18](#18-brin_redo--rm_brin_id--17) |
| 18 | CommitTs | `commit_ts_redo` | [§19](#19-commit_ts_redo--rm_commit_ts_id--18) |
| 19 | ReplicationOrigin | `replorigin_redo` | [§20](#20-replorigin_redo--rm_replorigin_id--19) |
| 20 | Generic | `generic_redo` | [§21](#21-generic_redo--rm_generic_id--20) |
| 21 | LogicalMessage | `logicalmsg_redo` | [§22](#22-logicalmsg_redo--rm_logicalmsg_id--21) |

---

## 1. `xlog_redo` — RM_XLOG_ID = 0

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

---

## 2. `xact_redo` — RM_XACT_ID = 1

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

---

## 3. `smgr_redo` — RM_SMGR_ID = 2

### Identity

* **rmgr id**: `RM_SMGR_ID = 2`
* **rmgr name**: `"Storage"`
* **redo function**: `smgr_redo` at
  `src/backend/catalog/storage.c:965`
* **header**: declared in `src/include/catalog/storage_xlog.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x10` | `XLOG_SMGR_CREATE` | Create new relfilenode |
| `0x20` | `XLOG_SMGR_TRUNCATE` | Truncate relation to recorded block count |

Payload structs:

* `xl_smgr_create { RelFileLocator rlocator; ForkNumber forkNum; }`
* `xl_smgr_truncate { RelFileLocator rlocator; BlockNumber blkno; uint32 flags; }`

### State mutations

* Filesystem: `smgrcreate(rel, fork, false)` creates new fork files.
* Filesystem: `smgrtruncate(rel, fork, blkno)` shortens fork files.
* Buffers: `DropRelationsBuffers` evicts pages above truncation
  point.

### Hot-standby behavior

`SMGR_TRUNCATE` indirectly causes snapshot conflicts via the
heap-pruning that accompanies VACUUM TRUNCATE. The smgr record
itself does **not** signal conflicts.

### Idempotency / LSN-skip

* Create is idempotent — `smgrcreate` is a no-op if the file
  already exists.
* Truncate is idempotent — `smgrtruncate` to the same length is a
  no-op.
* Goes through buffer manager (`DropRelationsBuffers`); not via
  `XLogReadBufferForRedo` directly.

### Crash safety

After replay, the on-disk fork files match the post-truncation
size. Subsequent records that refer to truncated blocks will hit
`BLK_NOTFOUND` in `XLogReadBufferForRedo` if the relation is later
dropped, or read the correct (truncated) page otherwise.

### Example

`XLOG_SMGR_TRUNCATE { rel=base/16384/12345, fork=MAIN, blkno=10 }`:

1. `DropRelationsBuffers(rel, fork, blkno)` — invalidates pages
   ≥ 10 in shared buffers.
2. `smgrtruncate(rel, fork, blkno=10)` — `ftruncate` the file.

---

---

## 4. `clog_redo` — RM_CLOG_ID = 3

### Identity

* **rmgr id**: `RM_CLOG_ID = 3`
* **rmgr name**: `"CLOG"`
* **redo function**: `clog_redo` at
  `src/backend/access/transam/clog.c:1107`
* **header**: declared in `src/include/access/clog.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `CLOG_ZEROPAGE` | Zero a new clog page |
| `0x10` | `CLOG_TRUNCATE` | Advance `oldestClogXid` + truncate SLRU |

Payload structs:

* `int64 pageno` (CLOG_ZEROPAGE)
* `xl_clog_truncate { int64 pageno; TransactionId oldestXact; Oid oldestXactDb; }` (CLOG_TRUNCATE)

### State mutations

| Target | Action |
|--------|--------|
| `pg_xact/` SLRU | New page allocated and zeroed |
| `pg_xact/` SLRU | Older segments removed via `SimpleLruTruncate` |
| `TransamVariables->oldestClogXid` | Advanced |

### Hot-standby behavior

CLOG records do **not** signal recovery conflicts — visibility
implications come from the per-record commit/abort writes via
`xact_redo_commit/abort`, not from these housekeeping records.

### Idempotency / LSN-skip

* `ZEROPAGE` is idempotent — re-zeroing an already-zero page is a
  no-op.
* `TRUNCATE` is idempotent — truncating to a `oldestClogXid` that's
  already been reached is a no-op.
* Goes through SLRU, not buffer manager — no page-LSN check.

### Crash safety

The SLRU files reflect at least all xids ≤ replayed xid. CLOG slot
writes happen via `xact_redo_commit`/`xact_redo_abort`; these
records ensure the on-disk SLRU framing is kept in sync.

### Example

`CLOG_ZEROPAGE pageno=123`:

1. `slru_zero_page(SimpleLruZeroPage, 123)` — write page 123 of
   `pg_xact/` SLRU as zeros, mark dirty.

`CLOG_TRUNCATE pageno=120 oldestXact=...`:

1. `TransamVariables->oldestClogXid = oldestXact`.
2. `SimpleLruTruncate(pg_xact, 120)` — remove pg_xact segments
   older than page 120.

---

---

## 5. `dbase_redo` — RM_DBASE_ID = 4

### Identity

* **rmgr id**: `RM_DBASE_ID = 4`
* **rmgr name**: `"Database"`
* **redo function**: `dbase_redo` at
  `src/backend/commands/dbcommands.c:3270`
* **header**: declared in `src/include/commands/dbcommands_xlog.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_DBASE_CREATE_FILE_COPY` | Old-style: copydir(template, new) |
| `0x10` | `XLOG_DBASE_CREATE_WAL_LOG` | New-style: contents replayed via WAL |
| `0x20` | `XLOG_DBASE_DROP` | Drop database |

Payload structs:

* `xl_dbase_create_file_copy_rec`
* `xl_dbase_create_wal_log_rec`
* `xl_dbase_drop_rec`

### State mutations

* Filesystem: `mkdir(base/<dboid>)`, copy template, or `rmtree` on
  drop.
* Buffers: `DropDatabaseBuffers(dbid)` on drop.

### Hot-standby behavior

`XLOG_DBASE_DROP` calls **`ResolveRecoveryConflictWithDatabase`** —
sends `PROCSIG_RECOVERY_CONFLICT_DATABASE` to every backend
connected to the dropped database. Those backends `proc_exit(1)`
(can't recover; the DB is gone). No grace period — the database is
disappearing immediately, not waiting.

### Idempotency / LSN-skip

* CREATE is idempotent if directory already exists.
* DROP is idempotent — `rmtree` of nonexistent path is OK.
* No page-LSN check; operations are at the filesystem level.

### Crash safety

After replay, the database directory either exists (matching the
template, or its WAL-logged content) or has been removed.

### Example

`XLOG_DBASE_DROP { db_id=16384 }`:

1. `ResolveRecoveryConflictWithDatabase(16384)` — kicks all
   backends connected to db 16384 (no wait).
2. `DropDatabaseBuffers(16384)` — evicts all buffers in that DB.
3. `rmtree("base/16384")` — removes data files.

---

---

## 6. `tblspc_redo` — RM_TBLSPC_ID = 5

### Identity

* **rmgr id**: `RM_TBLSPC_ID = 5`
* **rmgr name**: `"Tablespace"`
* **redo function**: `tblspc_redo` at
  `src/backend/commands/tablespace.c:1511`
* **header**: declared in `src/include/commands/tablespace.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_TBLSPC_CREATE` | Create tablespace + symlink |
| `0x10` | `XLOG_TBLSPC_DROP` | Drop tablespace + symlink |

Payload structs:

* `xl_tblspc_create_rec { Oid ts_id; char ts_path[FLEXIBLE_ARRAY_MEMBER]; }`
* `xl_tblspc_drop_rec { Oid ts_id; }`

### State mutations

* Filesystem: `pg_tblspc/<ts_id>` symlink created/destroyed.
* Filesystem: target directory tree created/destroyed.

### Hot-standby behavior

`XLOG_TBLSPC_DROP` calls
**`ResolveRecoveryConflictWithTablespace(ts_id)`** which uses
`ResolveRecoveryConflictWithVirtualXIDs` (the standard wait-cancel
path) to clear backends with **temp files in the target
tablespace** (`GetConflictingVirtualXIDs(temp_namespace=ts_id)`).
Subject to `max_standby_*_delay`.

### Idempotency / LSN-skip

* CREATE is idempotent — `mkdir` of existing dir is OK; symlink is
  re-created.
* DROP is idempotent — `unlink` + `rmtree` of nonexistent path is OK.

### Crash safety

After replay, `pg_tblspc/<ts_id>` either exists pointing at the
right path or is gone.

### Example

`XLOG_TBLSPC_DROP { ts_id=16400 }`:

1. `ResolveRecoveryConflictWithTablespace(16400)` — wait up to
   `max_standby_*_delay` for backends with temp files in ts 16400
   to release; cancel any that don't.
2. `destroy_tablespace_directories(16400, true)` — recursive
   removal.
3. `unlink("pg_tblspc/16400")` — remove symlink.

---

## Source references

* `src/backend/catalog/storage.c:965` — `smgr_redo`
* `src/backend/commands/dbcommands.c:3270` — `dbase_redo`
* `src/backend/commands/tablespace.c:1511` — `tblspc_redo`
* `src/include/catalog/storage_xlog.h` — smgr structs
* `src/include/commands/dbcommands_xlog.h` — dbase structs
* `src/include/commands/tablespace.h` — tblspc structs

---

## 7. `multixact_redo` — RM_MULTIXACT_ID = 6

### Identity

* **rmgr id**: `RM_MULTIXACT_ID = 6`
* **rmgr name**: `"MultiXact"`
* **redo function**: `multixact_redo` at
  `src/backend/access/transam/multixact.c:3386`
* **header**: declared in `src/include/access/multixact.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_MULTIXACT_ZERO_OFF_PAGE` | Zero offsets SLRU page |
| `0x10` | `XLOG_MULTIXACT_ZERO_MEM_PAGE` | Zero members SLRU page |
| `0x20` | `XLOG_MULTIXACT_CREATE_ID` | Record new multixact |
| `0x30` | `XLOG_MULTIXACT_TRUNCATE_ID` | Truncate offsets+members SLRUs |

Payload structs:

* `xl_multixact_create { MultiXactId mid; MultiXactOffset moff;
  int32 nmembers; MultiXactMember members[FLEXIBLE_ARRAY_MEMBER]; }`
* `xl_multixact_truncate`

### State mutations

* `pg_multixact/offsets` SLRU
* `pg_multixact/members` SLRU
* `MultiXactState` shmem (`nextMXact`, `nextOffset`,
  `oldestMultiXactId`, `oldestMultiXactDB`)

### Hot-standby behavior

Lock-share visibility of multixact members is rebuilt from these
records on the standby, ensuring `MultiXactIdIsRunning` correctly
returns the per-multixact running set.

### Idempotency / LSN-skip

* All operations are idempotent (zero a page, write members at a
  known offset, advance counters).
* Goes through SLRU.

### Crash safety

After replay, `pg_multixact/{offsets,members}` reflect every
multixact created up to the replayed LSN, so visibility checks
work correctly.

---

---

## 8. `relmap_redo` — RM_RELMAP_ID = 7

* **redo function**: `relmap_redo` at
  `src/backend/utils/cache/relmapper.c:1096`
* **header**: `src/include/utils/relmapper.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_RELMAP_UPDATE` | Rewrite `pg_filenode.map` |

The relmap is the special pre-bootstrap mapping for shared
catalogs and a few others (see `RelationMapper`); it can't live
in `pg_class` because it must be readable before `pg_class` is
accessible.

### State mutations

`$PGDATA/global/pg_filenode.map` (shared) or
`$PGDATA/base/<dboid>/pg_filenode.map` (per-database).

### Hot-standby behavior

Forces relcache invalidation for mapped relations
(`RelationCacheInvalidate`), so standby backends re-read mapped
relfilenodes after replay.

### Idempotency / LSN-skip

The on-disk file is rewritten atomically (rename of temp file).
Replay is idempotent.

---

---

## 9. `standby_redo` — RM_STANDBY_ID = 8

### Identity

* **rmgr id**: `RM_STANDBY_ID = 8`
* **rmgr name**: `"Standby"`
* **redo function**: `standby_redo` at
  `src/backend/storage/ipc/standby.c:1159`
* **header**: declared in `src/include/storage/standby.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_STANDBY_LOCK` | Per-record list of AccessExclusiveLocks |
| `0x10` | `XLOG_RUNNING_XACTS` | Snapshot of primary's procarray |
| `0x20` | `XLOG_INVALIDATIONS` | Standalone-inval message broadcast |

### Payload structs (`src/include/storage/standby.h`)

```c
typedef struct xl_standby_lock
{
    TransactionId   xid;        /* primary xid that owns the lock */
    Oid             dbOid;
    Oid             relOid;
} xl_standby_lock;

typedef struct xl_standby_locks
{
    int             nlocks;
    xl_standby_lock locks[FLEXIBLE_ARRAY_MEMBER];
} xl_standby_locks;

typedef struct xl_running_xacts
{
    int             xcnt;
    int             subxcnt;
    bool            subxid_overflow;
    TransactionId   nextXid;
    TransactionId   oldestRunningXid;
    TransactionId   latestCompletedXid;
    TransactionId   xids[FLEXIBLE_ARRAY_MEMBER];
} xl_running_xacts;

typedef struct xl_invalidations
{
    Oid             dbId;
    Oid             tsId;
    bool            relcacheInitFileInval;
    int             nmsgs;
    SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xl_invalidations;
```

### State mutations

| Target | Action | Triggered by |
|--------|--------|--------------|
| Lock manager | `StandbyAcquireAccessExclusiveLock` (per-lock) | `XLOG_STANDBY_LOCK` |
| KnownAssignedXids | Reset and repopulate | `XLOG_RUNNING_XACTS` |
| `standbyState` | INITIALIZED → SNAPSHOT_READY (or PENDING) | `XLOG_RUNNING_XACTS` |
| `pg_subtrans` | Subxid → parent mappings | `XLOG_RUNNING_XACTS` |
| sinval queue | `ProcessCommittedInvalidationMessages` | `XLOG_INVALIDATIONS` |

### Hot-standby behavior

This **is** the rmgr for hot-standby setup. It is the source of:

* All virtual locks the standby holds on behalf of primary
  transactions.
* The `KnownAssignedXids` snapshot used by every `GetSnapshotData`
  on the standby.
* Catalog-invalidation messages from primary-side
  `StartTransactionCommand`s (so standby backends notice DDL).

It also gates the `STANDBY_INITIALIZED → SNAPSHOT_PENDING →
SNAPSHOT_READY` state machine. Until SNAPSHOT_READY, no
hot-standby query can run.

### Conflict generation

`XLOG_STANDBY_LOCK` may emit `PROCSIG_RECOVERY_CONFLICT_LOCK` via
`StandbyAcquireAccessExclusiveLock` when `ProcSleep` decides the
backend already holds a conflicting lock. See
[recovery_conflict_catalog/lock_conflicts.md](../recovery_conflict_catalog/lock_conflicts.md).

### Idempotency / LSN-skip

* `XLOG_STANDBY_LOCK`: idempotent — re-acquiring the same virtual
  lock is a no-op.
* `XLOG_RUNNING_XACTS`: idempotent — repopulating
  KnownAssignedXids from the same set yields the same state.
* `XLOG_INVALIDATIONS`: idempotent — re-broadcasting inval
  messages is harmless (consumers handle duplicates).
* No data-page writes; no page-LSN check.

### Crash safety

`standby_redo` does not produce any new on-disk durability
guarantees — its mutations are all in shared memory. Crash safety
is established by:

* `XLOG_STANDBY_LOCK` — locks are auto-released at recovery exit
  by `StandbyReleaseAllLocks`, OR re-replayed from WAL on the next
  recovery.
* `XLOG_RUNNING_XACTS` — the standby's KnownAssignedXids is
  re-built on every restart by replaying these records.

### Example records

### Example 1: `XLOG_STANDBY_LOCK`

```
xl_standby_locks { nlocks=1, locks=[{xid=12345, dbOid=16384, relOid=20001}] }
```

`standby_redo` calls `StandbyAcquireAccessExclusiveLock(12345,
16384, 20001)`. Standby backends now block waiting for that virtual
lock to be released.

### Example 2: `XLOG_RUNNING_XACTS`

```
xl_running_xacts { xcnt=2, subxcnt=0, subxid_overflow=false,
                   nextXid=12350, oldestRunningXid=12340,
                   latestCompletedXid=12339,
                   xids=[12345, 12346] }
```

`standby_redo` calls `ProcArrayApplyRecoveryInfo(running)`:

1. Reset `KnownAssignedXids` ring.
2. Add 12345, 12346.
3. Update `latestCompletedXid = 12339`,
   `nextXid = max(nextXid, 12350)`.
4. `standbyState = STANDBY_SNAPSHOT_READY`.
5. Broadcast that hot standby is now consistent
   (`PMSIGNAL_BEGIN_HOT_STANDBY`).

### Example 3: `XLOG_INVALIDATIONS`

```
xl_invalidations { dbId=16384, tsId=1663, nmsgs=3, msgs=[...] }
```

`standby_redo` calls
`ProcessCommittedInvalidationMessages(msgs, 3)` — broadcasts to
shared invalidation queue. Standby backends will notice the
catalog change at next CFI.

This record is emitted by the primary in
`StartTransactionCommand` when the in-flight transaction has
already broadcast invalidation messages but has not yet committed.
The "early broadcast" feature prevents the standby from missing
inval messages on long-running primary transactions.

---

### Source references

* `src/backend/storage/ipc/standby.c:1159` — `standby_redo`
* `src/backend/storage/ipc/standby.c` — `StandbyAcquireAccessExclusiveLock`
* `src/backend/storage/ipc/standby.c` — `LogStandbySnapshot`
  (primary side)
* `src/backend/storage/ipc/procarray.c` — `ProcArrayApplyRecoveryInfo`,
  `ProcArrayApplyXidAssignment`,
  `ExpireTreeKnownAssignedTransactionIds`
* `src/include/storage/standby.h` — `xl_standby_lock`,
  `xl_running_xacts`, `xl_invalidations`
* `src/backend/storage/ipc/sinval.c` —
  `ProcessCommittedInvalidationMessages`

---

## 10. `heap2_redo` — RM_HEAP2_ID = 9

### Identity

* **rmgr id**: `RM_HEAP2_ID = 9`
* **rmgr name**: `"Heap2"`
* **redo function**: `heap2_redo` at
  `src/backend/access/heap/heapam.c:10384`
* **header**: declared in `src/include/access/heapam_xlog.h`

### Handled records

| Info | Constant | Per-record helper |
|------|----------|-------------------|
| `0x10` | `XLOG_HEAP2_PRUNE_ON_ACCESS` | `heap_xlog_prune_freeze` (opportunistic prune from a SELECT) |
| `0x20` | `XLOG_HEAP2_PRUNE_VACUUM_SCAN` | `heap_xlog_prune_freeze` (vacuum scan phase) |
| `0x30` | `XLOG_HEAP2_PRUNE_VACUUM_CLEANUP` | `heap_xlog_prune_freeze` (vacuum cleanup phase) |
| `0x40` | `XLOG_HEAP2_VISIBLE` | `heap_xlog_visible` (set VM all-visible bit) |
| `0x50` | `XLOG_HEAP2_MULTI_INSERT` | `heap_xlog_multi_insert` (COPY/INSERT bulk) |
| `0x60` | `XLOG_HEAP2_LOCK_UPDATED` | `heap_xlog_lock_updated` (subtle locking case) |
| `0x70` | `XLOG_HEAP2_NEW_CID` | logical-decoding only; no-op in redo |
| `0x80` | `XLOG_HEAP2_REWRITE` | `heap_xlog_logical_rewrite` (CLUSTER/VACUUM FULL) |

### State mutations

* Heap pages (data and Visibility Map fork).
* KnownAssignedXids — indirectly via
  `ResolveRecoveryConflictWithSnapshot`.

### Hot-standby behavior

PRUNE_* and VISIBLE both call
**`ResolveRecoveryConflictWithSnapshot(snapshotConflictHorizon)`**
before applying changes:

* PRUNE_*: any tuple version that was visible to a snapshot with
  `xmin < snapshotConflictHorizon` may have been physically
  removed. Backends with such snapshots are signaled and possibly
  cancelled.
* VISIBLE: setting the VM all-visible bit indicates no in-doubt
  tuples on the page. Backends snapshotting old data must be
  cleared.

The conflict goes through
`ResolveRecoveryConflictWithVirtualXIDs` with
`PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`, subject to
`max_standby_*_delay`.

### Idempotency / LSN-skip

* All paths go through `XLogReadBufferForRedo`.
* PRUNE_* records are LSN-checked. Replaying a prune that's
  already on-disk is a no-op.

### Crash safety

After replay:

* Pruned tuples are gone.
* VM bits are correctly set.
* Multi-insert: all tuples are placed on their target pages.
* Logical rewrite: pages are physically rewritten (CLUSTER /
  VACUUM FULL).

### Example: `XLOG_HEAP2_PRUNE_VACUUM_SCAN`

`heap_xlog_prune_freeze` body:

1. Extract `snapshotConflictHorizon` from the record.
2. **`ResolveRecoveryConflictWithSnapshot(snapshotConflictHorizon, rnode)`** —
   may signal & wait for backends.
3. `XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, false, &buf)`.
4. If `BLK_NEEDS_REDO`: walk the redirect/dead/unused arrays from
   the record, apply `PageRepairFragmentation`-equivalent surgery
   to the page.
5. Update VM bit if requested.
6. `PageSetLSN; MarkBufferDirty; UnlockReleaseBuffer`.

This is the canonical example of how a single redo dispatch can
cause a **standby query to be canceled**: a long-running SELECT on
the standby holds an old snapshot; the primary VACUUMs and emits
`XLOG_HEAP2_PRUNE_VACUUM_SCAN`; the standby's heap2_redo sees the
horizon, walks procarray, finds the SELECT's vxid, signals
`PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`, waits up to
`max_standby_streaming_delay`, then ERRORs the SELECT.

---

## Heap masking (for `wal_consistency_checking`)

`heap_mask` (in `heapam_xlog.c`) masks volatile fields before page
comparison: hint bits (`HEAP_XMIN_COMMITTED`, `HEAP_XMAX_COMMITTED`,
etc.), `pd_lsn`, `pd_checksum`. This is what makes
`wal_consistency_checking=on` work — the just-replayed page is
masked, the FPI is masked, and `memcmp` checks they match.

---

## Source references

* `src/backend/access/heap/heapam.c:10338` — `heap_redo`
* `src/backend/access/heap/heapam.c:10384` — `heap2_redo`
* `src/backend/access/heap/heapam_xlog.c` — `heap_xlog_*` helpers,
  `heap_mask`
* `src/backend/access/heap/visibilitymap.c` — VM update helpers
* `src/include/access/heapam_xlog.h` — `XLOG_HEAP_*`,
  `XLOG_HEAP2_*` constants and payload structs

---

## 11. `heap_redo` — RM_HEAP_ID = 10

### Identity

* **rmgr id**: `RM_HEAP_ID = 10`
* **rmgr name**: `"Heap"`
* **redo function**: `heap_redo` at
  `src/backend/access/heap/heapam.c:10338`
* **header**: declared in `src/include/access/heapam_xlog.h`

### Handled records

Mask: `XLOG_HEAP_OPMASK = 0x70`.

| Info | Constant | Per-record helper |
|------|----------|-------------------|
| `0x00` | `XLOG_HEAP_INSERT` | `heap_xlog_insert` |
| `0x10` | `XLOG_HEAP_DELETE` | `heap_xlog_delete` |
| `0x20` | `XLOG_HEAP_UPDATE` | `heap_xlog_update` |
| `0x30` | `XLOG_HEAP_TRUNCATE` | (no-op in redo; logical-decoding only; smgr_redo does the actual work) |
| `0x40` | `XLOG_HEAP_HOT_UPDATE` | `heap_xlog_hot_update` |
| `0x50` | `XLOG_HEAP_CONFIRM` | `heap_xlog_confirm` (speculative-insert confirm) |
| `0x60` | `XLOG_HEAP_LOCK` | `heap_xlog_lock` |
| `0x70` | `XLOG_HEAP_INPLACE` | `heap_xlog_inplace` |

### State mutations

* Heap pages: tuple insertion/deletion/update via
  `XLogReadBufferForRedo` + `MarkBufferDirty` + `PageSetLSN`.
* `pd_lower`/`pd_upper` line pointer fields adjusted.

### Hot-standby behavior

`heap_redo` itself does **not** signal recovery conflicts. That is
`heap2_redo`'s job — heap_redo's records do not invalidate
snapshots (each modification's xid will appear in a future
COMMIT/ABORT, where snapshot bookkeeping is handled).

### Idempotency / LSN-skip

* All heap_redo paths go through `XLogReadBufferForRedo` and
  obey the `BLK_DONE` skip when `page->pd_lsn >= record_lsn`.
* The bug-prone case is `HEAP_LOCK` (it modifies the tuple's
  xmax/infomask without changing visibility) — still LSN-skipped.

### Crash safety

After replay, the heap page contents match what the primary
intended. Buffer dirty + page LSN ensures the buffer manager will
later flush, advancing minRecoveryPoint.

### Example: `XLOG_HEAP_INSERT`

```
xl_heap_insert { OffsetNumber offnum; uint8 flags; }
+ tuple data on page block 0
```

`heap_xlog_insert` body:

1. `XLogReadBufferForRedo(record, 0, &buf)`:
   * If `BLK_RESTORED` (FPI was carried) — page already has the
     tuple; nothing to do.
   * If `BLK_NEEDS_REDO` — go to step 2.
   * If `BLK_DONE` / `BLK_NOTFOUND` — skip.
2. Get xlog data via `XLogRecGetBlockData` — gives raw tuple
   header + data.
3. Place tuple at `offnum` using `PageAddItem`.
4. `PageSetLSN(page, record->EndRecPtr); MarkBufferDirty(buf);`
5. `UnlockReleaseBuffer(buf)`.

---

---

## 12. `btree_redo` — RM_BTREE_ID = 11

### Identity

* **rmgr id**: `RM_BTREE_ID = 11`
* **rmgr name**: `"Btree"`
* **redo function**: `btree_redo` at
  `src/backend/access/nbtree/nbtxlog.c:1014`
* **rm_startup**: `btree_xlog_startup` (init incomplete-split tracker)
* **rm_cleanup**: `btree_xlog_cleanup` (finish leftover splits)
* **header**: declared in `src/include/access/nbtxlog.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_BTREE_INSERT_LEAF` | Insert into leaf |
| `0x10` | `XLOG_BTREE_INSERT_UPPER` | Insert into internal page |
| `0x20` | `XLOG_BTREE_INSERT_META` | Insert touches meta page |
| `0x30` | `XLOG_BTREE_SPLIT_L` | Split left page kept |
| `0x40` | `XLOG_BTREE_SPLIT_R` | Split right page kept |
| `0x50` | `XLOG_BTREE_INSERT_POST` | Posting-list insert |
| `0x60` | `XLOG_BTREE_DEDUP` | Deduplication |
| `0x70` | `XLOG_BTREE_DELETE` | Delete from leaf — **emits snapshot conflict** |
| `0x80` | `XLOG_BTREE_UNLINK_PAGE` | Unlink (after VACUUM marked half-dead) |
| `0x90` | `XLOG_BTREE_UNLINK_PAGE_META` | Unlink updates meta |
| `0xA0` | `XLOG_BTREE_NEWROOT` | New root after split |
| `0xB0` | `XLOG_BTREE_MARK_PAGE_HALFDEAD` | Page becoming dead |
| `0xC0` | `XLOG_BTREE_VACUUM` | Bulk delete during VACUUM |
| `0xD0` | `XLOG_BTREE_REUSE_PAGE` | Reuse page — **emits snapshot conflict horizon** |

Payload structs (in `nbtxlog.h`):

* `xl_btree_insert`
* `xl_btree_split` / `xl_btree_split_alt`
* `xl_btree_dedup`
* `xl_btree_delete`
* `xl_btree_reuse_page`
* `xl_btree_unlink_page`
* `xl_btree_metadata`
* `xl_btree_newroot`

### State mutations

* B-tree index pages.
* A separate `incomplete_split` hash table maintained across the
  redo loop (initialized by `rm_startup`, drained by
  `rm_cleanup`).

### Incomplete-split tracker

A B-tree split logs:

1. `XLOG_BTREE_SPLIT_L` (or `_R`): both leaf pages have the new
   split layout, but the parent has not yet been updated.
2. `XLOG_BTREE_INSERT_UPPER` later: the parent page learns about
   the new right sibling.

If recovery sees only step 1 (because the cluster crashed before
step 2 was emitted), the tree is *temporarily inconsistent* — the
right sibling exists but no parent points to it.

`btree_xlog_startup` allocates a hash table keyed by `(rel,
left-block)`. `btree_redo` for SPLIT inserts an entry; for
INSERT_UPPER removes it. `btree_xlog_cleanup` walks any leftover
entries and finishes the parent update by calling
`_bt_finish_split` directly on the index.

### Hot-standby behavior

* `XLOG_BTREE_DELETE`: emits
  `ResolveRecoveryConflictWithSnapshot(snapshotConflictHorizon)`.
* `XLOG_BTREE_REUSE_PAGE`: emits
  `ResolveRecoveryConflictWithSnapshot(latestRemovedFullXid)`.

These conflicts are needed because index tuples being removed
might still be visible to a backend's snapshot — the heap-level
conflict (`XLOG_HEAP2_PRUNE_*`) is not always sufficient because
the index can be pruned independently of the heap.

### Idempotency / LSN-skip

* All page modifications go through `XLogReadBufferForRedo` with
  LSN-skip.
* The incomplete-split tracker is *not* idempotent across
  recovery runs — but `rm_startup` initializes it from scratch
  each time, so re-replaying SPLIT records gives the right
  end state.

### Crash safety

After `rm_cleanup` runs, the B-tree is consistent: every leaf
split has a corresponding parent entry. Index scans see the
correct structure.

### Example: `XLOG_BTREE_DELETE`

```c
xl_btree_delete {
    TransactionId snapshotConflictHorizon;
    uint16       ndeleted;
    uint16       nupdated;
    /* Followed by deleted offset numbers + updated offset numbers. */
}
```

`btree_redo` for DELETE:

1. Extract `snapshotConflictHorizon`.
2. **`ResolveRecoveryConflictWithSnapshot(horizon, rnode)`** —
   wait/cancel backends with old snapshots.
3. `XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, false, &buf)`.
4. If `BLK_NEEDS_REDO`:
   * Read deleted offsets array from xlog data.
   * For each deleted offset: `PageIndexTupleDelete`.
   * For each updated offset (posting-list update): apply the new
     posting list bytes.
   * `PageSetLSN; MarkBufferDirty`.
5. Release buffer.

---

### Source references

* `src/backend/access/nbtree/nbtxlog.c:1014` — `btree_redo`
* `src/backend/access/nbtree/nbtxlog.c` — `btree_xlog_startup`,
  `btree_xlog_cleanup`, `btree_xlog_delete`,
  `btree_xlog_reuse_page`, `_bt_finish_split`
* `src/include/access/nbtxlog.h` — `XLOG_BTREE_*` constants and
  payload structs

---

## 13. `hash_redo` — RM_HASH_ID = 12

* **redo function**: `hash_redo` at
  `src/backend/access/hash/hash_xlog.c:1067`
* **header**: `src/include/access/hash_xlog.h`

### Handled records

`XLOG_HASH_INIT_META_PAGE`, `XLOG_HASH_INIT_BITMAP_PAGE`,
`XLOG_HASH_INSERT`, `XLOG_HASH_ADD_OVFL_PAGE`,
`XLOG_HASH_SPLIT_ALLOCATE_PAGE`, `XLOG_HASH_SPLIT_PAGE`,
`XLOG_HASH_SPLIT_COMPLETE`, `XLOG_HASH_MOVE_PAGE_CONTENTS`,
`XLOG_HASH_SQUEEZE_PAGE`, `XLOG_HASH_DELETE`,
`XLOG_HASH_UPDATE_META_PAGE`, `XLOG_HASH_VACUUM_ONE_PAGE`.

### State mutations

Hash index pages.

### Hot-standby behavior

`XLOG_HASH_VACUUM_ONE_PAGE` emits
`ResolveRecoveryConflictWithSnapshot(latestRemovedXid)` — same
mechanism as btree DELETE/REUSE_PAGE.

### Idempotency / LSN-skip

All paths through `XLogReadBufferForRedo`.

---

---

## 14. `gin_redo` — RM_GIN_ID = 13

* **redo function**: `gin_redo` at
  `src/backend/access/gin/ginxlog.c:726`
* **rm_startup**: `gin_xlog_startup`
* **rm_cleanup**: `gin_xlog_cleanup`
* **header**: `src/include/access/ginxlog.h`

### Handled records

`XLOG_GIN_CREATE_PTREE`, `XLOG_GIN_INSERT`, `XLOG_GIN_SPLIT`,
`XLOG_GIN_VACUUM_PAGE`, `XLOG_GIN_VACUUM_DATA_LEAF_PAGE`,
`XLOG_GIN_DELETE_PAGE`, `XLOG_GIN_UPDATE_META_PAGE`,
`XLOG_GIN_INSERT_LISTPAGE`, `XLOG_GIN_DELETE_LISTPAGE`.

### State mutations

GIN index pages, posting trees/lists.

### Hot-standby behavior

No direct conflict — GIN VACUUM relies on the heap-level
`XLOG_HEAP2_PRUNE_*` records to issue snapshot conflicts.

### Incomplete-split tracker

Like btree, GIN tracks incomplete splits via the rm_startup/
rm_cleanup hooks.

---

---

## 15. `gist_redo` — RM_GIST_ID = 14

* **redo function**: `gist_redo` at
  `src/backend/access/gist/gistxlog.c:397`
* **rm_startup**: `gist_xlog_startup`
* **rm_cleanup**: `gist_xlog_cleanup`
* **header**: `src/include/access/gistxlog.h`

### Handled records

`XLOG_GIST_PAGE_UPDATE`, `XLOG_GIST_DELETE`,
`XLOG_GIST_PAGE_REUSE`, `XLOG_GIST_PAGE_SPLIT`,
`XLOG_GIST_ASSIGN_LSN`, `XLOG_GIST_PAGE_DELETE`.

### State mutations

GiST pages.

### Hot-standby behavior

`XLOG_GIST_PAGE_REUSE` emits snapshot-conflict horizon via
`ResolveRecoveryConflictWithSnapshotFullXid` — index page being
reused had references that an old snapshot might still be using.

---

---

## 16. `seq_redo` — RM_SEQ_ID = 15

* **redo function**: `seq_redo` at
  `src/backend/commands/sequence.c:1834`
* **header**: `src/include/commands/sequence.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_SEQ_LOG` | Copy sequence tuple onto page |

### State mutations

Sequence relation page (a heap with one tuple). The replay
overwrites the page with the recorded tuple+state.

### Hot-standby behavior

None — sequences are not subject to snapshot conflicts (they're
not transactional in the MVCC sense).

### Idempotency / LSN-skip

Goes through `XLogReadBufferForRedo`; LSN-skipped.

---

---

## 17. `spg_redo` — RM_SPGIST_ID = 16

* **redo function**: `spg_redo` at
  `src/backend/access/spgist/spgxlog.c:935`
* **rm_startup**: `spg_xlog_startup`
* **rm_cleanup**: `spg_xlog_cleanup`
* **header**: `src/include/access/spgxlog.h`

### Handled records

`XLOG_SPGIST_ADD_LEAF`, `XLOG_SPGIST_MOVE_LEAFS`,
`XLOG_SPGIST_ADD_NODE`, `XLOG_SPGIST_SPLIT_TUPLE`,
`XLOG_SPGIST_PICKSPLIT`, `XLOG_SPGIST_VACUUM_LEAF`,
`XLOG_SPGIST_VACUUM_ROOT`, `XLOG_SPGIST_VACUUM_REDIRECT`.

### State mutations

SP-GiST pages, redirect tombstone state.

### Hot-standby behavior

`XLOG_SPGIST_VACUUM_REDIRECT` emits snapshot-conflict horizon —
SP-GiST uses redirect tombstones to handle concurrent
vacuum/scan; replaying the cleanup of a tombstone is unsafe for
old snapshots.

---

---

## 18. `brin_redo` — RM_BRIN_ID = 17

* **redo function**: `brin_redo` at
  `src/backend/access/brin/brin_xlog.c:309`
* **header**: `src/include/access/brin_xlog.h`

### Handled records

`XLOG_BRIN_CREATE_INDEX`, `XLOG_BRIN_INSERT`,
`XLOG_BRIN_UPDATE`, `XLOG_BRIN_SAMEPAGE_UPDATE`,
`XLOG_BRIN_REVMAP_EXTEND`, `XLOG_BRIN_DESUMMARIZE`.

### State mutations

BRIN regular pages, BRIN revmap pages.

### Hot-standby behavior

No direct conflict — BRIN summary updates don't invalidate
visibility because BRIN entries are summary data, not tuple
versions.

---

## Common pattern

Every index AM redo callback follows the same skeleton:

```c
static void
amxx_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /* For records that may invalidate snapshots */
    if (info needs snapshot conflict)
        ResolveRecoveryConflictWithSnapshot(horizon, ...);

    switch (info) {
        case XLOG_AMXX_OP1: amxx_xlog_op1(record); break;
        case XLOG_AMXX_OP2: amxx_xlog_op2(record); break;
        ...
        default: elog(PANIC, "amxx_redo: unknown op code %u", info);
    }
}

static void amxx_xlog_op1(XLogReaderState *record)
{
    Buffer buf;
    if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO) {
        Page page = BufferGetPage(buf);
        /* op-specific page surgery */
        PageSetLSN(page, record->EndRecPtr);
        MarkBufferDirty(buf);
    }
    if (BufferIsValid(buf)) UnlockReleaseBuffer(buf);
}
```

All AMs except BRIN emit at least one snapshot-conflict path; all
AMs except hash and BRIN have rm_startup/rm_cleanup for
incomplete-operation tracking.

---

## Source references

* `src/backend/access/hash/hash_xlog.c:1067` — `hash_redo`
* `src/backend/access/gin/ginxlog.c:726` — `gin_redo`
* `src/backend/access/gist/gistxlog.c:397` — `gist_redo`
* `src/backend/access/spgist/spgxlog.c:935` — `spg_redo`
* `src/backend/access/brin/brin_xlog.c:309` — `brin_redo`
* Headers in `src/include/access/{hash,gin,gist,spg,brin}_xlog.h`

---

## 19. `commit_ts_redo` — RM_COMMIT_TS_ID = 18

### Identity

* **rmgr id**: `RM_COMMIT_TS_ID = 18`
* **rmgr name**: `"CommitTs"`
* **redo function**: `commit_ts_redo` at
  `src/backend/access/transam/commit_ts.c:1023`
* **header**: declared in `src/include/access/commit_ts.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `COMMIT_TS_ZEROPAGE` | Zero a new commit_ts page |
| `0x10` | `COMMIT_TS_TRUNCATE` | Truncate commit_ts SLRU |

Payload structs: same shape as CLOG variants.

### State mutations

* `pg_commit_ts/` SLRU pages.

### Hot-standby behavior

Replicates commit-timestamp visibility. The actual per-xid commit
timestamp is written by `xact_redo_commit` via
`TransactionTreeSetCommitTsData`, gated on
`track_commit_timestamp=on`.

### Idempotency / LSN-skip

* Same as CLOG — idempotent SLRU writes.

### Crash safety

Same as CLOG. The SLRU file framing is kept in sync; commit-ts
data is written by `xact_redo_commit` ensuring per-xid coverage.

---

## Source references

* `src/backend/access/transam/clog.c:1107` — `clog_redo`
* `src/backend/access/transam/multixact.c:3386` — `multixact_redo`
* `src/backend/access/transam/commit_ts.c:1023` — `commit_ts_redo`
* `src/include/access/clog.h` — `CLOG_ZEROPAGE`, `CLOG_TRUNCATE`
* `src/include/access/multixact.h` — `XLOG_MULTIXACT_*`
* `src/include/access/commit_ts.h` — `COMMIT_TS_ZEROPAGE`,
  `COMMIT_TS_TRUNCATE`

---

## 20. `replorigin_redo` — RM_REPLORIGIN_ID = 19

* **redo function**: `replorigin_redo` at
  `src/backend/replication/logical/origin.c:827`
* **header**: `src/include/replication/origin.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_REPLORIGIN_SET` | `replorigin_advance(node, lsn)` |
| `0x10` | `XLOG_REPLORIGIN_DROP` | Remove origin entry |

### State mutations

`pg_replication_origin` progress in shared memory + on-disk
`pg_logical/replorigin_checkpoint`.

### Hot-standby behavior

No direct implications. Matters for cascaded logical
replication: a cascaded standby replays origin advances so it can
honor `replorigin_session_origin` filtering when its own logical
walsender emits changes.

### Idempotency / LSN-skip

Origin progress is LSN-monotone; advancing to an already-passed
LSN is a no-op.

---

---

## 21. `generic_redo` — RM_GENERIC_ID = 20

* **redo function**: `generic_redo` at
  `src/backend/access/transam/generic_xlog.c:478`
* **header**: `src/include/access/generic_xlog.h`

### Handled records

A single record type that carries a list of page-deltas (start
offset, length, payload bytes) recorded via `generic_xlog.c` API
(`GenericXLogStart`, `GenericXLogRegisterBuffer`, etc.).

### State mutations

Arbitrary buffer pages owned by the extension that emitted the
record.

### Hot-standby behavior

Extension-defined; default safe — generic_xlog records do not
emit conflict signals. Extensions that need to invalidate
snapshots must use a custom rmgr instead.

### Idempotency / LSN-skip

The deltas reference target buffers via the standard block-ref
mechanism, so they go through `XLogReadBufferForRedo` and obey
LSN-skip.

### Use case

Used by extensions that need WAL logging but don't want a custom
rmgr. The classic in-tree user is `bloom` (a `contrib/` index AM)
which uses generic_xlog for its WAL records.

---

---

## 22. `logicalmsg_redo` — RM_LOGICALMSG_ID = 21

* **redo function**: `logicalmsg_redo` at
  `src/backend/replication/logical/message.c:87`
* **header**: `src/include/replication/message.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_LOGICAL_MESSAGE` | No-op in redo (decoding only) |

### State mutations

None on the redo path. The record exists purely for logical
decoding consumers (`pg_logical_emit_message` SQL function).

### Hot-standby behavior

None.

### Idempotency / LSN-skip

Trivially idempotent (no-op).

---

## Source references

* `src/backend/commands/sequence.c:1834` — `seq_redo`
* `src/backend/replication/logical/origin.c:827` — `replorigin_redo`
* `src/backend/utils/cache/relmapper.c:1096` — `relmap_redo`
* `src/backend/access/transam/generic_xlog.c:478` — `generic_redo`
* `src/backend/replication/logical/message.c:87` — `logicalmsg_redo`

---


## Cross-references

* For the dispatch mechanism: [14_rmgr_dispatch.md](14_rmgr_dispatch.md).
* For the buffer-helper contract used by every page-modifying redo callback: [15_recovery_buffer_helpers.md](15_recovery_buffer_helpers.md).
* For the hot-standby integration of `standby_redo`: [10_hot_standby_and_recovery_conflicts.md](10_hot_standby_and_recovery_conflicts.md).
* For the recovery conflicts triggered by `heap2_redo`, `btree_redo`, etc.: [18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md).
* For the snapshot-conflict horizon as it appears in heap/btree records: [appendix_data_structures.md](appendix_data_structures.md).
* One-line-per-callback overview: [appendix_redo_callback_quick_reference.md](appendix_redo_callback_quick_reference.md).