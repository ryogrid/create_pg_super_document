# WAL Record Catalog: XLOG rmgr (Checkpoints, NEXTOID, FPI)

`RM_XLOG_ID = "XLOG"`, redo: `xlog_redo`
(`src/backend/access/transam/xlog.c`).

This rmgr handles the cluster-wide control records.

## XLOG_CHECKPOINT_SHUTDOWN  (info 0x00)

- **Header**: `pg_control.h:68`.
- **Payload**: `CheckPoint` struct (`pg_control.h:35`).
  ```c
  typedef struct CheckPoint
  {
      XLogRecPtr        redo;
      TimeLineID        ThisTimeLineID;
      TimeLineID        PrevTimeLineID;
      bool              fullPageWrites;
      int               wal_level;
      FullTransactionId nextXid;
      Oid               nextOid;
      MultiXactId       nextMulti;
      MultiXactOffset   nextMultiOffset;
      TransactionId     oldestXid;
      Oid               oldestXidDB;
      MultiXactId       oldestMulti;
      Oid               oldestMultiDB;
      pg_time_t         time;
      TransactionId     oldestCommitTsXid;
      TransactionId     newestCommitTsXid;
      TransactionId     oldestActiveXid;
  } CheckPoint;
  ```
- **Emitter**: `CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | ...)`.
- **Redo**: `xlog_redo` updates `ShmemVariableCache` cursors
  (nextXid, nextOid, nextMulti, ..., oldestCommitTsXid). After this record
  is processed, the standby's WAL replay can be considered "consistent" if
  there were no pending in-flight transactions at shutdown.
- **Makes durable**: a complete snapshot of cluster metadata cursors.

## XLOG_CHECKPOINT_ONLINE  (info 0x10)

- **Header**: `pg_control.h:69`.
- **Payload**: same `CheckPoint` struct, but with `oldestActiveXid` set
  (used to initialize hot-standby snapshot building).
- **Emitter**: `CreateCheckPoint` (without IS_SHUTDOWN flag) — the
  routine periodic checkpoint.
- **Redo**: same as SHUTDOWN, but the standby uses `oldestActiveXid` to
  prime its KnownAssignedXids state for hot standby.

## XLOG_NEXTOID  (info 0x30)

- **Header**: `pg_control.h:71`.
- **Payload**: `Oid nextOidValue` (4 bytes).
- **Emitter**: `GetNewObjectId()` — every `VAR_OID_PREFETCH` (currently 8192)
  OIDs allocated, the next "checkpoint" OID is logged so a crash never
  hands out the same OID twice.
- **Redo**: `xlog_redo` advances `ShmemVariableCache->nextOid` to at least
  the logged value.
- **Makes durable**: the OID counter advance.

## XLOG_FPI  (info 0xB0)

- **Header**: `pg_control.h:79`.
- **Payload**: empty (the data is in the registered buffer's full-page image).
- **Emitter**: ad-hoc — used by VACUUM page-prune in some cases, and other
  routines that need to inject an FPI for non-WAL-logged page changes.
- **Redo**: `xlog_redo` restores the FPI to the page.
- **Makes durable**: a torn-page-safe initial image for a page about to be
  modified by a non-WAL-logged change.

## XLOG_FPI_FOR_HINT  (info 0xA0)

- **Header**: `pg_control.h:78`.
- **Payload**: empty (FPI in the registered buffer).
- **Emitter**: `MarkBufferDirtyHint` when checksums or `wal_log_hints` is
  enabled. Triggered by hint-bit-only changes (e.g., setting
  `HEAP_XMIN_COMMITTED`).
- **Redo**: same as XLOG_FPI — restore the page from FPI.
- **Makes durable**: the page contents at the moment of the hint-bit
  write, so a torn write does not corrupt non-hint data.

## XLOG_CHECKPOINT_REDO  (info 0xE0)

- **Header**: `pg_control.h:82`.
- **Payload**: small marker (no useful data — the LSN of the record itself
  is the data).
- **Emitter**: `CreateCheckPoint` immediately after computing the redo
  pointer. Inserted at the precise WAL location that becomes the recovery
  start point.
- **Redo**: no-op.
- **Makes durable**: the precise LSN at which redo will start. Important
  because the actual XLOG_CHECKPOINT_* record can be far later (the
  checkpoint flushes all dirty buffers, which can take minutes).

## XLOG_NOOP  (info 0x20)

- **Payload**: variable padding bytes.
- **Emitter**: `XLogInsert(XLOG_NOOP)` for pg_walfile_name boundary alignment.
- **Redo**: no-op.

## XLOG_SWITCH  (info 0x40)

Forces switching to a new WAL segment. Used by base backup.

## XLOG_BACKUP_END  (info 0x50)

Marks the end of a backup; used by `pg_backup_stop`.

## XLOG_PARAMETER_CHANGE  (info 0x60)

- **Payload**: changes to wal-level-relevant GUCs.
- **Emitter**: when ALTER SYSTEM SET wal_level (or a few others) takes effect.
- **Redo**: standby errors out if the parameter change reduces wal_level
  below what the standby needs (e.g., changing from `replica` to `minimal`).

## XLOG_RESTORE_POINT  (info 0x70)

User-named restore point for PITR.

## XLOG_FPW_CHANGE  (info 0x80)

Tracks `full_page_writes` toggling.

## XLOG_END_OF_RECOVERY  (info 0x90)

Inserted at the end of crash recovery.

## XLOG_OVERWRITE_CONTRECORD  (info 0xD0)

Used to overwrite a continuation-record header when an aborted WAL
record was partially written.

## Cross-references

- `component_checkpoints_and_recovery.md` — checkpoint creation flow.
- `component_persistence_and_wal_records.md` — XLOG_FPI_FOR_HINT context.
