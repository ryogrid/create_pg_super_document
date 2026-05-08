# 16 — Checkpoints and Recovery

[Up: index.md](index.md)  |  [Prev: 15 persistence and wal records](15_persistence_and_wal_records.md)  |  [Next: 17 hooks and extensibility](17_hooks_and_extensibility.md)


## Prerequisites

- [15](15_persistence_and_wal_records.md) — the WAL records being checkpointed and replayed.

## Overview

Two operations bracket every cluster's lifetime:

- **Checkpoint**: a point-in-WAL where every dirty page in the system has
  been flushed to disk, and `pg_control` has been updated with the
  cluster-wide cursors. After a checkpoint, recovery can start replaying WAL
  from the checkpoint's redo pointer.
- **Recovery (StartupXLOG)**: at backend startup, read pg_control, set
  cursors, replay WAL from the redo pointer to the end (or to a stop target).

This component documents how the metadata subsystems plug into both.

## Checkpoint

### CreateCheckPoint  (importance 0.93, Tier 1)

`xlog.c::CreateCheckPoint(int flags)`. Top-level driver, called by:

- the checkpointer auxiliary process (every `checkpoint_timeout` or when WAL
  exceeds `max_wal_size`),
- explicit `CHECKPOINT` SQL,
- shutdown sequence (`flags & CHECKPOINT_IS_SHUTDOWN`).

**Logic**:

1. **Acquire CheckpointLock**. Only one checkpoint at a time.
2. **Compute redo pointer**. Take `WALInsertLock` exclusive to freeze WAL
   inserts briefly; use `XLogCtl->Insert.RedoRecPtr` as the redo pointer.
3. **Insert XLOG_CHECKPOINT_REDO**: a tiny WAL marker at the precise redo
   point, so recovery starts at a well-defined record boundary.
4. **Build CheckPoint struct**:
   ```c
   CheckPoint cp;
   cp.redo            = RedoRecPtr;
   cp.ThisTimeLineID  = ThisTimeLineID;
   cp.PrevTimeLineID  = PrevTimeLineID;
   cp.fullPageWrites  = current FPW setting;
   cp.wal_level       = wal_level;
   cp.nextXid         = ShmemVariableCache->nextXid;
   cp.nextOid         = ShmemVariableCache->nextOid;
   cp.nextMulti       = MultiXactState->nextMXact;
   cp.nextMultiOffset = MultiXactState->nextOffset;
   cp.oldestXid       = ShmemVariableCache->oldestXid;
   cp.oldestXidDB     = ShmemVariableCache->oldestXidDB;
   cp.oldestMulti     = MultiXactState->oldestMulti;
   cp.oldestMultiDB   = MultiXactState->oldestMultiDB;
   cp.time            = now();
   cp.oldestCommitTsXid = commitTsShared->oldestCommitTsXid;
   cp.newestCommitTsXid = commitTsShared->newestCommitTsXid;
   cp.oldestActiveXid = (online checkpoint && hot_standby) ? GetOldestActiveTransactionId() : InvalidTransactionId;
   ```
5. **Call CheckPointGuts(RedoRecPtr, flags)** — the inner dispatcher.
6. **Insert XLOG_CHECKPOINT_SHUTDOWN or XLOG_CHECKPOINT_ONLINE** with the
   CheckPoint struct as payload.
7. **XLogFlush(checkpoint_lsn)**.
8. **UpdateControlFile()** — writes the new ControlFileData with
   `state = DB_IN_PRODUCTION` (or DB_SHUTDOWNED), `time = now()`,
   `checkPoint = checkpoint_lsn`, `checkPointCopy = cp`,
   `minRecoveryPoint = ...`, etc.
9. Truncate / archive WAL up to the new redo point if appropriate.

### CheckPointGuts  (importance 0.95, Tier 1)

**Signature** (`xlog.c`):
```c
static void CheckPointGuts(XLogRecPtr checkPointRedo, int flags);
```

**The dispatch order**:

```c
CheckPointRelationMap();                /* relmapper.c — flush pending relmap */
CheckPointReplicationSlots(...);
CheckPointSnapBuild();
CheckPointLogicalRewriteHeap();
CheckPointReplicationOrigin();
CheckPointCLOG();                       /* SimpleLruWriteAll(XactCtl)        */
CheckPointCommitTs();                   /* SimpleLruWriteAll(CommitTsCtl)    */
CheckPointSUBTRANS();                   /* SimpleLruWriteAll(SubTransCtl)    */
CheckPointMultiXact();                  /* both MultiXact SLRUs             */
CheckPointPredicate();                  /* Serial SLRU                      */
CheckPointBuffers(flags);               /* bufmgr.c — flush every dirty page */
ProcessSyncRequests();                  /* fsync everything in queue         */
CheckPointTwoPhase(checkPointRedo);     /* persisted prepared xacts          */
```

The order matters in two ways:

1. **Relmap before SLRUs**: a relmap update may rename pg_filenode.map, which
   if half-applied could orphan an SLRU file. Doing relmap first fences
   subsequent operations.
2. **SLRUs before Buffers**: an SLRU page write may trigger a sync request
   that the checkpointer batches with `ProcessSyncRequests`. Doing SLRUs
   before the main buffers means the sync queue is fully populated by the
   time we run ProcessSyncRequests.
3. **Buffers before TwoPhase**: 2PC files reference relfilenodes; we want
   their data on disk before we declare 2PC stable.

### UpdateControlFile  (importance 0.82, Tier 1)

`xlog.c`:
```c
void UpdateControlFile(void);
```

**Logic**:

1. Take `ControlFileLock` exclusive.
2. `ControlFile->time = (pg_time_t) time(NULL)`.
3. Compute CRC: `INIT_CRC32C; COMP_CRC32C(crc, ControlFile, offsetof(crc));
   FIN_CRC32C; ControlFile->crc = crc`.
4. `pg_pwrite(controlFile, ControlFile, sizeof(ControlFileData), 0)`.
5. `pg_fsync(controlFile)`.
6. Release lock.

The write is one sector → atomic on common hardware. The CRC catches any
silent corruption. The time field is not security-critical but helps
operators correlate logs.

### Frequency and triggering

- **Periodic**: every `checkpoint_timeout` seconds (default 5 min), the
  checkpointer auxiliary process triggers a checkpoint.
- **WAL-driven**: when `pg_wal_size > checkpoint_completion_target *
  max_wal_size`, the checkpointer accelerates.
- **Explicit**: `CHECKPOINT` SQL command (must be superuser).
- **Shutdown**: `pg_ctl stop` triggers a `SHUTDOWN CHECKPOINT`.
- **Restartpoint** (standby): standbys cannot create true checkpoints
  during recovery; instead they create *restartpoints* at every
  XLOG_CHECKPOINT_ONLINE record they replay.

## Recovery

### StartupXLOG  (importance 0.95, Tier 1)

`xlogrecovery.c::StartupXLOG()`. Top-level recovery driver, called once at
startup process initialization.

**Phases**:

1. **Read pg_control**: `ReadControlFile()` populates `ControlFile`.
2. **Validate**:
   - magic + CRC.
   - `pg_control_version == PG_CONTROL_VERSION`.
   - `catalog_version_no == CATALOG_VERSION_NO` (compile-time).
   - architecture compatibility (maxAlign, blcksz, etc.).
3. **Determine recovery mode**:
   - DB_SHUTDOWNED: clean restart, no replay needed.
   - DB_IN_PRODUCTION / DB_SHUTDOWNING / DB_IN_CRASH_RECOVERY: replay needed.
   - presence of `recovery.signal` or `standby.signal`: archive recovery
     or hot standby.
4. **Set in-memory cursors** from `ControlFile->checkPointCopy`:
   - `ShmemVariableCache->nextXid` = checkPointCopy.nextXid.
   - `ShmemVariableCache->nextOid` = checkPointCopy.nextOid.
   - `MultiXactSetNextMXact(nextMulti, nextMultiOffset)`.
   - `SetTransactionIdLimit(oldestXid, oldestXidDB)`.
   - `SetMultiXactIdLimit(oldestMulti, oldestMultiDB)`.
5. **Run BootStrap*/Startup* hooks**:
   - `StartupCLOG()`.
   - `StartupCommitTs()`.
   - `StartupSUBTRANS(oldestActiveXID)`.
   - `StartupMultiXact()`.
6. **PerformWalRecovery**: open WAL starting from `checkPointCopy.redo`,
   read records, dispatch via `rmgr[record->rmid]->rm_redo(record)`.
7. **Reach end of WAL** (or recovery target).
8. **TrimCLOG()**: zero out the trailing portion of the live CLOG page.
9. **TrimMultiXact()**: same for offsets+members.
10. **Mark consistent**: set `ControlFile->state = DB_IN_PRODUCTION`,
    `UpdateControlFile`.
11. **Open for connections**: signal Postmaster.

### ReadControlFile  (importance 0.78)

`xlog.c`:
```c
void ReadControlFile(void);
```

**Logic**:
1. Open `$PGDATA/global/pg_control`.
2. Read `sizeof(ControlFileData)` bytes.
3. Validate magic, CRC, pg_control_version, catalog_version_no, architecture.
4. ereport FATAL if any check fails.

### What if pg_control is corrupt?

`pg_control` corruption is the worst kind of cluster-level damage. Options:

1. Restore from backup.
2. `pg_resetwal` — last-resort tool that synthesizes a new pg_control. Will
   lose any committed transactions whose WAL has not been applied.

The 512-byte size is intended to maximize the chance that the write is
atomic. CRC validation catches any partial writes or bit rot.

### TrimCLOG / TrimMultiXact  (post-recovery cleanup)

After recovery, the live tail of CLOG / MultiXact may have stale data left
from the crashed transaction. Trimming zeros it:

```c
void TrimCLOG(void)
{
    int64    pageno   = TransactionIdToPage(nextXid);
    int      slotno   = SimpleLruReadPage(XactCtl, pageno, true, nextXid);
    char    *byteptr  = page_buffer[slotno] + TransactionIdToByte(nextXid);
    /* Zero from current XID position to end of page */
    memset(byteptr, 0, BLCKSZ - TransactionIdToByte(nextXid));
    page_dirty = true;
}
```

### TrimMultiXact

Similar, but on both offsets and members SLRUs.

## The consistency point

A standby reaches "consistent" when its replay LSN ≥ `minRecoveryPoint`. This
is the latest point at which any data block was flushed during the prior
recovery; replay must reach at least that LSN before connections are allowed.

## Restartpoint

A restartpoint is the standby's analog of a checkpoint:

```c
void CreateRestartPoint(int flags);
```

Called when the standby's startup process replays an XLOG_CHECKPOINT_ONLINE
record. Logic:

1. Validate that we have replayed past the checkpoint's redo pointer.
2. Run `CheckPointGuts(checkpoint_redo, flags)` — flush our own dirty buffers.
3. Update pg_control's `checkPoint = lsn_of_replayed_checkpoint`,
   `minRecoveryPoint = max(replay_lsn, minRecoveryPoint)`.

After a restartpoint, the standby could be promoted with low recovery cost
(only WAL after the restartpoint must be replayed).

## Cross-references

- `[15 Persistence and WAL Records](15_persistence_and_wal_records.md)` — record-by-record details.
- `[09 CLOG](09_clog.md)`, `[11 Commit Timestamps](11_commit_timestamps.md)`, `[12 MultiXact](12_multixact.md)`,
  `[07 Relmapper](07_relmapper.md)`, `[10 SUBTRANS](10_subtrans.md)` — per-subsystem
  Bootstrap*/Startup*/Trim*/CheckPoint* hooks.
- `[03 Catalog Data Model](03_catalog_data_model_and_bootstrap.md)` — pg_control fields.

## Source references

- `src/backend/access/transam/xlog.c::CreateCheckPoint`
- `src/backend/access/transam/xlog.c::CheckPointGuts`
- `src/backend/access/transam/xlog.c::UpdateControlFile`
- `src/backend/access/transam/xlog.c::ReadControlFile`
- `src/backend/access/transam/xlog.c::CreateRestartPoint`
- `src/backend/access/transam/xlogrecovery.c::StartupXLOG`
- `src/backend/access/transam/xlogrecovery.c::PerformWalRecovery`
- `src/backend/access/transam/clog.c::StartupCLOG`,
  `TrimCLOG`, `CheckPointCLOG`
- `src/backend/access/transam/commit_ts.c::StartupCommitTs`,
  `CheckPointCommitTs`
- `src/backend/access/transam/multixact.c::StartupMultiXact`,
  `TrimMultiXact`, `CheckPointMultiXact`
- `src/backend/access/transam/subtrans.c::StartupSUBTRANS`,
  `CheckPointSUBTRANS`
- `src/backend/utils/cache/relmapper.c::CheckPointRelationMap`

---

[Up: index.md](index.md)  |  [Prev](15_persistence_and_wal_records.md)  |  [Next](17_hooks_and_extensibility.md)
