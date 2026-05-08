# 01 — Executive Summary

[Up: index.md](index.md)  |  [Next: 02 Architecture Overview](02_architecture_overview.md)

## What is the metadata subsystem?

PostgreSQL's metadata subsystem is the **self-describing layer** of the
database: every relation, every column, every type, every function,
every dependency, every transaction's commit status, every page's
all-visible hint, and every cluster-wide cursor (next XID, next OID,
oldest multi, oldest commit-timestamp XID, …) lives somewhere in this
subsystem. Without it, the executor cannot decode a single tuple.

The subsystem spans four data domains, each backed by a different
durability strategy, and they are stitched together by a single
**WAL/checkpoint/pg_control spine** that anchors the cluster across
restarts, crashes, and replication:

```
                ┌─────────────────────────────────────────────┐
                │   pg_control (global/pg_control)             │
                │   = cluster anchor, atomic 512-byte file     │
                └──────────────┬──────────────────────────────┘
                               │ checkpoint cursors
                               ▼
        ┌──────────┬──────────────┬────────────┬──────────────┐
        │ Catalogs │ Commit-log   │ Visibility │ Free-space   │
        │ (pg_*)   │ family       │ Map (VM)   │ Map (FSM)    │
        │          │ (CLOG, multi,│            │              │
        │          │  CommitTs,   │            │              │
        │          │  SUBTRANS,   │            │              │
        │          │  relmap)     │            │              │
        └──────────┴──────────────┴────────────┴──────────────┘
                               ▲
                               │
                          WAL stream
                          (rmgrlist.h)
```

## The four data domains

### Domain 1: System catalogs — `pg_catalog`

**Sixty-three** ordinary heap relations describe everything the cluster
knows about itself: relations (`pg_class`), columns (`pg_attribute`),
types (`pg_type`), functions (`pg_proc`), dependencies (`pg_depend`,
`pg_shdepend`), constraints (`pg_constraint`), namespaces
(`pg_namespace`), schemas, partitions, statistics, ACLs, replication
state, foreign data wrappers, text search dictionaries, and so on.

These tables live in two flavours:

- **Local**: 52 catalogs — one physical relation per database, lives
  in `base/<dboid>/<relfilenode>`.
- **Shared**: 11 catalogs (`pg_authid`, `pg_database`, `pg_tablespace`,
  …) — one physical relation visible from every database, lives in
  `global/<relfilenode>`.

Four catalogs are *nailed* (`pg_class`, `pg_attribute`, `pg_proc`,
`pg_type`) — their schema is hard-coded in `formrdesc` so the relcache
can read them before any catalog is open. Every nailed catalog and
every shared catalog is also *mapped* — its filenode is recorded in
the relmapper's `pg_filenode.map` rather than `pg_class.relfilenode`,
to break the bootstrap circularity.

**Durability strategy**: full WAL — every catalog row update produces
a normal heap WAL record, plus the transaction's `XLOG_XACT_COMMIT`
broadcasts the corresponding catalog cache invalidations. See
chapters [03](03_catalog_data_model_and_bootstrap.md),
[04](04_catalog_modification_apis.md),
[05](05_catalog_caches.md), [06](06_cache_invalidation.md),
[07](07_relmapper.md), [18](18_catalog_inventory.md).

### Domain 2: The commit-log family — SLRU-backed metadata

A small family of "Simple LRU" caches each backed by a numbered
on-disk directory, used for fixed-size per-XID or per-MultiXactId
records:

| SLRU         | What it stores                       | WAL?                          |
|--------------|--------------------------------------|--------------------------------|
| **CLOG**     | 2 bits/XID — commit/abort status     | yes (zero-page, truncate)      |
| **CommitTs** | 10 bytes/XID — timestamp + origin    | yes, gated by GUC              |
| **MultiXact**| two SLRUs: offsets + members         | yes (zero, create, truncate)   |
| **SUBTRANS** | 4 bytes/XID — subtransaction parent  | **no** — runtime-reconstructable|
| **Notify**   | LISTEN/NOTIFY queue                  | no (volatile)                  |
| **Serial**   | predicate-lock SeqNo per XID         | no (volatile)                  |

Plus the **relmapper** (`pg_filenode.map`) — not strictly an SLRU but
fits the same "small fixed-size persistent file with a WAL record"
pattern. See chapters [07](07_relmapper.md), [08](08_slru_framework.md),
[09](09_clog.md) – [12](12_multixact.md), [19](19_slru_users_catalog.md).

### Domain 3: The Visibility Map (VM)

Two bits per heap page — `ALL_VISIBLE` and `ALL_FROZEN` — kept in a
separate fork of every heap relation (`VM_FORKNUM`). They enable two
crucial optimizations: index-only scans skip the heap fetch when the
target page is `ALL_VISIBLE`, and aggressive vacuum skips
`ALL_FROZEN` pages entirely.

**Durability**: bit-set is durable (logged via `XLOG_HEAP2_VISIBLE`
with an LSN-ordering invariant). Bit-clear is implicit in the heap
mutation WAL record (every `heap_xlog_*` mutating record's redo
clears the affected VM bit, so the clear is "free" in WAL terms).
See chapter [13](13_visibility_map.md).

### Domain 4: The Free Space Map (FSM)

One byte per heap page — a coarse "how many bytes are free here"
category. Stored as a three-level tree of pages in another fork
(`FSM_FORKNUM`), with a binary heap inside each page. Drives the
heap-extension fast path: when an INSERT needs space, FSM tells
hio.c which page to try first.

**Durability**: hint-only. FSM is allowed to lie; vacuum repairs.
No FSM-specific WAL records (apart from a hint-FPI when checksums or
`wal_log_hints` is on). See chapter [14](14_free_space_map.md).

## The unifying spine: WAL, checkpoint, pg_control

Every persistent metadata change is anchored by three structures
working together:

1. **WAL records** (`rmgrlist.h` dispatch). Thirty distinct
   metadata-affecting record types live across nine resource managers
   (XLOG, XACT, SMGR, CLOG, MULTIXACT, COMMIT_TS, RELMAP, HEAP2,
   DBASE, TBLSPC). See chapter [15](15_persistence_and_wal_records.md)
   and the [WAL record catalog](20_wal_record_catalog.md).

2. **Checkpoint** (`CheckPointGuts`, `xlog.c:7504`). A single
   integration point that flushes every metadata subsystem in a
   carefully chosen order: relmap → CLOG → CommitTs → SUBTRANS →
   MultiXact → Predicate → Buffers → SyncRequests → TwoPhase. After a
   checkpoint, recovery can start replaying WAL from the redo
   pointer. See chapter [16](16_checkpoints_and_recovery.md).

3. **pg_control** (`global/pg_control`, atomic 512-byte file). The
   cluster anchor: holds `system_identifier`, `pg_control_version`,
   `catalog_version_no`, `state`, the latest checkpoint LSN, and the
   inline `CheckPoint` struct with every metadata cursor (`nextXid`,
   `nextOid`, `nextMulti`, `nextMultiOffset`, `oldestXid`,
   `oldestMulti`, `oldestCommitTsXid`, `newestCommitTsXid`,
   `oldestActiveXid`). See chapter
   [03](03_catalog_data_model_and_bootstrap.md) §
   "pg_control as the recovery anchor".

## Strict durability vs hint-style metadata

The crucial trade-off, repeated throughout this document:

| Style               | Used by                         | What "lost" means                                    |
|---------------------|---------------------------------|------------------------------------------------------|
| **Strict (WAL)**    | catalog rows, CLOG, MultiXact, CommitTs (when on), relmap, smgr-create | data becomes unreadable, recovery fails              |
| **Reconstructable** | SUBTRANS, sinval queue          | rebuilt from runtime state on next demand            |
| **Hint (best-effort)** | FSM, heap-tuple infomask hint bits, parts of VM (PD_ALL_VISIBLE) | quality of service degrades; vacuum repairs         |
| **Embedded**        | sinval messages (in xl_xact_commit), checkpoint cursors (in pg_control) | recovered alongside their carrier                  |

The hint-style structures are *not* dispensable — they are crucial for
performance — but they are explicitly allowed to be wrong, which is
why they need very little WAL traffic.

## Key APIs (the most-used metadata calls)

For the impatient (full reference in
[metadata_api_reference.md](metadata_api_reference.md)):

```c
/* Catalog modification (every DDL goes through these) */
void  CatalogTupleInsert (Relation heapRel, HeapTuple tup);
void  CatalogTupleUpdate (Relation heapRel, ItemPointer otid, HeapTuple tup);
void  CatalogTupleDelete (Relation heapRel, ItemPointer tid);

/* Catalog reading (every SELECT/parse step goes through these) */
HeapTuple SearchSysCache1(int cacheId, Datum key1);
void      ReleaseSysCache(HeapTuple tuple);
Relation  RelationIdGetRelation(Oid relid);
void      RelationClose(Relation rel);

/* Visibility Map */
void   visibilitymap_set   (Relation, BlockNumber heapBlk, Buffer heapBuf,
                            XLogRecPtr recptr, Buffer vmBuf,
                            TransactionId cutoff_xid, uint8 flags);
bool   visibilitymap_clear (Relation, BlockNumber heapBlk, Buffer vmbuf, uint8 flags);

/* Free Space Map */
BlockNumber GetPageWithFreeSpace      (Relation, Size spaceNeeded);
void        RecordPageWithFreeSpace   (Relation, BlockNumber heapBlk, Size spaceAvail);

/* CLOG */
void      TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
                                     TransactionId *subxids,
                                     XidStatus status, XLogRecPtr lsn);
XidStatus TransactionIdGetStatus    (TransactionId xid, XLogRecPtr *lsn);

/* MultiXact */
MultiXactId MultiXactIdCreate(TransactionId xid1, MultiXactStatus status1,
                              TransactionId xid2, MultiXactStatus status2);
int         GetMultiXactIdMembers(MultiXactId multi, MultiXactMember **members,
                                  bool from_pgupgrade, bool isLockOnly);
```

## Reading order

Follow the [Reading order](index.md#how-to-read-this-document) in the
index. New readers benefit most from:

1. This page (you are here).
2. [02 Architecture Overview](02_architecture_overview.md) — the
   one-page system diagram.
3. [03 Catalog Data Model and Bootstrap](03_catalog_data_model_and_bootstrap.md) — what a catalog *is*.
4. Then jump to whichever subsystem you need.

The detailed catalog chapters ([18](18_catalog_inventory.md),
[19](19_slru_users_catalog.md), [20](20_wal_record_catalog.md)) are
optimized for **lookup**, not sequential reading.

---

[Up: index.md](index.md)  |  [Next: 02 Architecture Overview](02_architecture_overview.md)
