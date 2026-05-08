# 15 — Persistence and WAL Records

[Up: index.md](index.md)  |  [Prev: 14 free space map](14_free_space_map.md)  |  [Next: 16 checkpoints and recovery](16_checkpoints_and_recovery.md)


## Prerequisites

- Chapters 03–14 — every domain whose WAL records are catalogued here.

## Overview

This document is the integration narrative: how the various metadata
subsystems achieve durability, and the catalog of WAL records that touch
metadata.

## Four durability strategies

| Strategy                                         | Used by                                                |
|--------------------------------------------------|--------------------------------------------------------|
| (a) WAL-logged changes                           | CLOG, CommitTs, MultiXact, RelMap, XACT, SMGR, HEAP    |
| (b) Reconstructable from runtime state           | SUBTRANS, sinval queue                                 |
| (c) Hint structures rebuilt on demand            | FSM (mostly), parts of VM (PD_ALL_VISIBLE bit)         |
| (d) Embedded in WAL records or in pg_control     | sinval messages (in xl_xact_commit), checkpoint cursors |

## The buffer manager as synchronization boundary

Every persistent buffer (heap, index, VM, FSM, SLRU page) has an LSN field.
The **WAL-before-data rule** says: a buffer's payload may not be flushed to
disk until WAL up to its LSN has been flushed.

`bufmgr.c::FlushBuffer`:
```c
XLogFlush(BufferGetLSNAtomic(buffer));
smgrwrite(reln, forkNum, blockNum, BufferGetBlock(buffer), false);
```

`SimpleLruWritePage` enforces the same rule via `group_lsn` for CLOG.

## The transaction commit pathway in detail

### RecordTransactionCommit

`xact.c`. Sequence:

1. **Build the message list**:
   `nmsgs = xactGetCommittedInvalidationMessages(&msgs, &RelcacheInitFileInval)`.
2. **Build the relfilelocator drop list**: walk pendingDeletes for entries
   marked `atCommit = true && atDelete = false`.
3. **Build xl_xact_commit**:
   ```c
   xl_xact_commit xlrec;
   xlrec.xact_time = ...;
   xlrec.nsubxacts = ...;
   xlrec.nrels = ...;
   xlrec.nmsgs = nmsgs;
   ...
   ```
   Add subsidiary structs (`xl_xact_xinfo`, `xl_xact_dbinfo`,
   `xl_xact_subxacts`, `xl_xact_relfilelocators`, `xl_xact_invals`,
   `xl_xact_twophase`, `xl_xact_origin`).
4. **Insert WAL record**: `XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT)`.
5. **Synchronous flush**: if `synchronous_commit >= REMOTE_FLUSH`, await
   sync replication. Otherwise `XLogFlush(commit_lsn)` if synchronous_commit
   is on; skip flush for off (async commit).
6. **Update CLOG**: `TransactionIdCommitTree(xid, nsubxids, subxids)`.
7. **Update CommitTs**: `TransactionTreeSetCommitTsData(...)`.
8. **End the transaction in ProcArray**: `ProcArrayEndTransaction`. Now
   other backends see this xid as "committed" via snapshot.
9. **Apply local invalidations**: `AtEOXact_Inval(true)` — local cache
   flush plus push to sinval ring.
10. **smgrDoPendingDeletes(true)**: unlink files marked for deletion at
    commit.
11. **Other AtEOXact_* hooks**: relmap, MultiXact cache, plancache, ...

The ordering is critical:

- WAL → flush → CLOG bit. A crash between step 5 and step 6 leaves the
  XID in WAL but not in CLOG — but `xact_redo_commit` will re-apply during
  recovery.
- CLOG bit → ProcArrayEndTransaction. This means the moment a backend can
  observe "this xid is gone from running list", the CLOG bit is already set,
  so visibility checks see COMMITTED.
- ProcArrayEndTransaction → AtEOXact_Inval. Other backends start to use
  the new state in step 8; they receive sinval in step 9.

### The redo pathway: xact_redo_commit  (importance 0.85, Tier 1)

`xact.c`:
```c
static void xact_redo_commit(xl_xact_parsed_commit *parsed,
                             TransactionId xid, XLogRecPtr lsn,
                             RepOriginId origin_id);
```

**Logic** (mirroring the primary):
1. `TransactionIdCommitTree(xid, parsed->nsubxacts, parsed->subxacts)`.
2. If `xinfo & XACT_XINFO_HAS_INVALS`:
   `ProcessCommittedInvalidationMessages(parsed->msgs, parsed->nmsgs,
   RelcacheInitFileInval, dbId, tsId)`.
3. `TransactionTreeSetCommitTsData(...)`.
4. `smgrDoPendingDeletes(true)` — execute the dropped relfilelocator
   unlinks.
5. ProcArray bookkeeping (advance latestCompletedXid, etc.).

The standby thus reaches the same final state as the primary, in the same
order.

### xact_redo_abort

```c
static void xact_redo_abort(xl_xact_parsed_abort *parsed,
                            TransactionId xid, XLogRecPtr lsn,
                            RepOriginId origin_id);
```

1. `TransactionIdAbortTree(xid, parsed->nsubxacts, parsed->subxacts)`.
2. `smgrDoPendingDeletes(false)` — undo file creates queued in this xact.
3. ProcArray bookkeeping.

## rmgrlist.h — the master dispatch table

`src/include/access/rmgrlist.h` lists every resource manager:

```c
PG_RMGR(RM_XLOG_ID,      "XLOG",        xlog_redo,       xlog_desc, ...)
PG_RMGR(RM_XACT_ID,      "Transaction", xact_redo,       xact_desc, ...)
PG_RMGR(RM_SMGR_ID,      "Storage",     smgr_redo,       smgr_desc, ...)
PG_RMGR(RM_CLOG_ID,      "CLOG",        clog_redo,       clog_desc, ...)
PG_RMGR(RM_DBASE_ID,     "Database",    dbase_redo,      dbase_desc, ...)
PG_RMGR(RM_TBLSPC_ID,    "Tablespace",  tblspc_redo,     tblspc_desc, ...)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact",   multixact_redo,  multixact_desc, ...)
PG_RMGR(RM_RELMAP_ID,    "RelMap",      relmap_redo,     relmap_desc, ...)
PG_RMGR(RM_STANDBY_ID,   "Standby",     standby_redo,    ...)
PG_RMGR(RM_HEAP2_ID,     "Heap2",       heap2_redo,      ...)
PG_RMGR(RM_HEAP_ID,      "Heap",        heap_redo,       ...)
PG_RMGR(RM_BTREE_ID,     "Btree",       btree_redo,      ...)
PG_RMGR(RM_HASH_ID,      "Hash",        hash_redo,       ...)
...
PG_RMGR(RM_COMMIT_TS_ID, "CommitTs",    commit_ts_redo,  commit_ts_desc, ...)
...
```

A WAL record's `rmid` field selects the row; the record's `info` byte
selects the action within that row's redo function.

`custom_rmgr` lets extensions register their own RM_* IDs in a reserved range.

## Metadata-affecting WAL records (per rmgr)

Every record listed in `wal_record_inventory.txt` has a dedicated entry in
`[20 WAL Record Catalog](20_wal_record_catalog.md) — see *.md`. Below is a brief cross-rmgr summary:

### RM_XLOG_ID

| info | name                    | payload                | makes durable                                    |
|------|-------------------------|------------------------|--------------------------------------------------|
| 0x00 | XLOG_CHECKPOINT_SHUTDOWN| CheckPoint              | end-of-shutdown checkpoint                        |
| 0x10 | XLOG_CHECKPOINT_ONLINE  | CheckPoint              | online checkpoint                                 |
| 0x30 | XLOG_NEXTOID            | Oid                     | OID counter advance (every VAR_OID_PREFETCH OIDs) |
| 0xA0 | XLOG_FPI_FOR_HINT       | full-page image          | hint-bit-only changes under checksums              |
| 0xB0 | XLOG_FPI                | full-page image          | torn-page-safe non-WAL-logged changes             |
| 0xE0 | XLOG_CHECKPOINT_REDO    | empty                   | precise redo-point marker                         |

### RM_XACT_ID

| info | name                     | makes durable                                                       |
|------|--------------------------|----------------------------------------------------------------------|
| 0x00 | XLOG_XACT_COMMIT         | CLOG, dropped relfilenodes, sinval messages, commit_ts, replication |
| 0x10 | XLOG_XACT_PREPARE        | 2PC PREPARE                                                          |
| 0x20 | XLOG_XACT_ABORT          | aborted XID tree, undo of file creates                               |
| 0x30 | XLOG_XACT_COMMIT_PREPARED| commit half of prepared transaction                                  |
| 0x40 | XLOG_XACT_ABORT_PREPARED | abort half                                                           |
| 0x50 | XLOG_XACT_ASSIGNMENT     | top-XID assignment for sub-XID list (snapshot building)              |
| 0x60 | XLOG_XACT_INVALIDATIONS  | mid-transaction invalidations (logical decoding)                     |

### RM_SMGR_ID  (smgr_redo, storage.c:965)

| info | name              | payload          | makes durable                          |
|------|-------------------|------------------|-----------------------------------------|
| 0x10 | XLOG_SMGR_CREATE  | xl_smgr_create   | physical relfilenode/fork creation      |
| 0x20 | XLOG_SMGR_TRUNCATE| xl_smgr_truncate | physical truncation; VM/FSM truncate    |

### RM_CLOG_ID  (clog_redo, clog.c:1107)

| info | name              | payload          | makes durable                  |
|------|-------------------|------------------|---------------------------------|
| 0x00 | XLOG_CLOG_ZEROPAGE| int64 pageno     | fresh CLOG page                 |
| 0x10 | XLOG_CLOG_TRUNCATE| xl_clog_truncate | CLOG truncation; oldestXid bump |

### RM_DBASE_ID

| info | name                          | payload                       | makes durable                          |
|------|-------------------------------|-------------------------------|-----------------------------------------|
| 0x00 | XLOG_DBASE_CREATE_FILE_COPY   | xl_dbase_create_file_copy_rec | CREATE DATABASE via filesystem copy     |
| 0x10 | XLOG_DBASE_CREATE_WAL_LOG     | xl_dbase_create_wal_log_rec   | CREATE DATABASE STRATEGY=WAL_LOG         |
| 0x20 | XLOG_DBASE_DROP               | xl_dbase_drop_rec             | DROP DATABASE                            |

### RM_TBLSPC_ID

| info | name              | payload                | makes durable                  |
|------|-------------------|------------------------|---------------------------------|
| 0x00 | XLOG_TBLSPC_CREATE| xl_tblspc_create_rec    | tablespace symlink creation     |
| 0x10 | XLOG_TBLSPC_DROP  | xl_tblspc_drop_rec      | tablespace symlink/dir removal  |

### RM_MULTIXACT_ID  (multixact_redo)

| info | name                         | payload             |
|------|------------------------------|---------------------|
| 0x00 | XLOG_MULTIXACT_ZERO_OFF_PAGE | int64 pageno        |
| 0x10 | XLOG_MULTIXACT_ZERO_MEM_PAGE | int64 pageno        |
| 0x20 | XLOG_MULTIXACT_CREATE_ID     | xl_multixact_create |
| 0x30 | XLOG_MULTIXACT_TRUNCATE_ID   | xl_multixact_truncate|

### RM_RELMAP_ID  (relmap_redo)

| info | name              | payload          |
|------|-------------------|------------------|
| 0x00 | XLOG_RELMAP_UPDATE| xl_relmap_update |

### RM_HEAP2_ID

| info | name                | payload          |
|------|---------------------|------------------|
| 0x40 | XLOG_HEAP2_VISIBLE  | xl_heap_visible  |

(Other RM_HEAP2 records — XLOG_HEAP2_PRUNE, XLOG_HEAP2_FREEZE_PAGE,
XLOG_HEAP2_LOCK_UPDATED, XLOG_HEAP2_MULTI_INSERT — implicitly clear VM
bits during their redo but are not "metadata" records per se.)

### RM_COMMIT_TS_ID  (commit_ts_redo, commit_ts.c:1023)

| info | name                   | payload              |
|------|------------------------|----------------------|
| 0x00 | XLOG_COMMIT_TS_ZEROPAGE| int64 pageno         |
| 0x10 | XLOG_COMMIT_TS_TRUNCATE| xl_commit_ts_truncate|
| 0x40? | XLOG_COMMIT_TS_SETTS  | xl_commit_ts_set     |

## Implicit metadata effects

Many ordinary records have side-effects on metadata structures during redo:

- `XLOG_XACT_COMMIT`'s redo updates CLOG (`TransactionIdCommitTree`) and
  CommitTs.
- `XLOG_XACT_ABORT`'s redo updates CLOG (`TransactionIdAbortTree`).
- `XLOG_HEAP_INSERT`, `_UPDATE`, `_DELETE`, `_LOCK`,
  `XLOG_HEAP2_MULTI_INSERT` redo functions all call `visibilitymap_clear`.
- `XLOG_HEAP2_PRUNE` updates `pg_class.reltuples` (in-place via
  `heap_inplace_update_and_unlock`).

## Hint bits and XLOG_FPI_FOR_HINT  (deep dive)

Heap tuples have `HEAP_XMIN_COMMITTED`, `HEAP_XMIN_INVALID`,
`HEAP_XMAX_COMMITTED`, `HEAP_XMAX_INVALID` infomask bits. Setting these
bits is a *hint* — they are derivable from CLOG. Writing a hint bit is
done via `MarkBufferDirtyHint`, which:

- under default settings: marks the page dirty without WAL,
- under `wal_log_hints = on` or `data_checksums = on`:
  emits `XLOG_FPI_FOR_HINT` so a torn page does not corrupt the bits.

The "torn page" hazard: a write of an 8 KiB page is non-atomic; if power
fails mid-write, half the page has the new bit, half has the old. Without
checksums the page is still readable (PostgreSQL trusts data even with no
torn-page protection if checksums are off). With checksums, the torn write
fails verification, so we need the FPI to allow the page to be reconstructed.

## XLOG_HEAP2_VISIBLE conditional FPI  (deep dive)

`xl_heap_visible` itself is small. The full-page-image of the VM page is
included when:

- The VM page's last LSN < checkpoint redo pointer (the standard "torn-page
  protection at first dirty after checkpoint" rule).
- Or `wal_log_hints` / data checksums are enabled (forces FPI for VM hints).

In high-throughput workloads, the FPI is emitted once per VM page per
checkpoint cycle. Subsequent visibilitymap_set calls on the same page
within the same checkpoint cycle skip the FPI — they only log the bit
changes.

## Cross-references

- `[16 Checkpoints and Recovery](16_checkpoints_and_recovery.md)` — checkpoint integration.
- `[09 CLOG](09_clog.md)`, `[11 Commit Timestamps](11_commit_timestamps.md)`, `[12 MultiXact](12_multixact.md)`,
  `[07 Relmapper](07_relmapper.md)`, `[13 Visibility Map](13_visibility_map.md)` — the per-subsystem
  WAL details.
- `[20 WAL Record Catalog](20_wal_record_catalog.md) — see *.md` — per-record formats.

## Source references

- `src/include/access/rmgrlist.h` — master rmgr table
- `src/backend/access/transam/xact.c::RecordTransactionCommit`
- `src/backend/access/transam/xact.c::xact_redo_commit`
- `src/backend/access/transam/xact.c::xact_redo_abort`
- `src/backend/access/transam/xlog.c::XLogInsert`, `XLogFlush`, `XLogBeginInsert`
- `src/backend/storage/buffer/bufmgr.c::FlushBuffer`,
  `MarkBufferDirtyHint`
- `src/backend/access/heap/heapam.c::heap_xlog_visible` (plus heap_xlog_*)
- `src/backend/catalog/storage.c::log_smgrcreate`,
  `RelationDropStorage`, `smgrDoPendingDeletes`

---

[Up: index.md](index.md)  |  [Prev](14_free_space_map.md)  |  [Next](16_checkpoints_and_recovery.md)
