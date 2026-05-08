# 02 — Architecture Overview

[Up: index.md](index.md)  |  [Prev: 01 Executive Summary](01_executive_summary.md)  |  [Next: 03 Catalog Data Model and Bootstrap](03_catalog_data_model_and_bootstrap.md)

This chapter is the system-wide map. It introduces every box in the
metadata subsystem and how each box interacts with the others. Detailed
treatment of each box happens in subsequent chapters.

## Prerequisites

- A casual familiarity with PostgreSQL's MVCC model (XIDs, snapshots,
  visibility checks).
- A casual familiarity with WAL (write-ahead log records, redo,
  checkpoints).
- Read [01 Executive Summary](01_executive_summary.md) first.

## The four data domains and the spine

```mermaid
flowchart TB
    subgraph SPINE["The unifying spine — runs across every domain"]
        WAL[("WAL stream<br/>rmgrlist.h dispatch")]
        CKPT["CheckPointGuts<br/>xlog.c:7504"]
        PGC[("pg_control<br/>global/pg_control<br/>atomic 512 B")]
        WAL --> CKPT
        CKPT --> PGC
        PGC -->|reboot| RECOV["StartupXLOG<br/>xlog.c:5384"]
        RECOV --> WAL
    end

    subgraph CAT["DOMAIN 1 — System Catalogs (pg_catalog)"]
        CATFILES[("63 heap relations<br/>11 shared, 4 nailed,<br/>15 mapped")]
        CATAPI["heap.c, index.c, indexing.c<br/>dependency.c, namespace.c<br/>storage.c, toasting.c, aclchk.c<br/>per-catalog pg_*.c helpers"]
        CACHES["catcache → syscache → relcache<br/>plancache, partcache, typcache, ..."]
        INVAL["inval.c → sinval ring buffer<br/>SI_RESET on overflow"]
        RELMAP[("pg_filenode.map<br/>shared + per-db")]
        CATAPI --> CATFILES
        CATFILES --> CACHES
        CATAPI --> INVAL
        INVAL --> CACHES
        RELMAP -.->|filenode lookup| CACHES
    end

    subgraph CL["DOMAIN 2 — Commit-log family (SLRU)"]
        SLRU["slru.c<br/>bank-locked page pool"]
        CLOG[("pg_xact<br/>2 b/XID")]
        SUBTRANS[("pg_subtrans<br/>4 B/XID")]
        CTS[("pg_commit_ts<br/>10 B/XID, GUC-gated")]
        MX[("pg_multixact/<br/>offsets+members")]
        SLRU --> CLOG
        SLRU --> SUBTRANS
        SLRU --> CTS
        SLRU --> MX
    end

    subgraph VM["DOMAIN 3 — Visibility Map"]
        VMFILE[("VM_FORKNUM<br/>2 b/heap-page<br/>ALL_VISIBLE+ALL_FROZEN")]
        VMAPI["visibilitymap_set/clear/<br/>pin/get_status<br/>vm_readbuf, vm_extend"]
        VMAPI --> VMFILE
    end

    subgraph FSM["DOMAIN 4 — Free Space Map"]
        FSMFILE[("FSM_FORKNUM<br/>1 B/heap-page<br/>3-level tree of pages")]
        FSMAPI["GetPageWithFreeSpace<br/>RecordPageWithFreeSpace<br/>fsm_search, fsm_set_avail"]
        HIO["hio.c<br/>RelationGetBufferForTuple"]
        FSMAPI --> FSMFILE
        HIO --> FSMAPI
        HIO --> VMAPI
    end

    CATAPI -.->|XLOG_SMGR_CREATE,<br/>XLOG_RELMAP_UPDATE,<br/>XLOG_XACT_COMMIT| WAL
    CLOG -.->|XLOG_CLOG_*| WAL
    CTS -.->|XLOG_COMMIT_TS_*| WAL
    MX -.->|XLOG_MULTIXACT_*| WAL
    SUBTRANS -.->|no WAL| WAL
    VMFILE -.->|XLOG_HEAP2_VISIBLE| WAL
    FSMFILE -.->|XLOG_FPI_FOR_HINT only| WAL
    RELMAP -.->|XLOG_RELMAP_UPDATE| WAL

    CKPT --> CACHES
    CKPT --> CLOG
    CKPT --> CTS
    CKPT --> SUBTRANS
    CKPT --> MX
    CKPT --> RELMAP
```

This single diagram is worth memorizing. Every later chapter elaborates
one of these boxes; the spine on top is what makes them all crash-safe
and replicable as a unit.

## End-to-end persistence pipeline

The most useful slice through the system is the path of one DDL
statement from SQL to durable on-disk catalog row to standby cache
flush. This is the only diagram that shows every spine cooperation in
action:

```mermaid
flowchart TB
    subgraph BACKEND["Backend (in-transaction)"]
        DDL["SQL DDL<br/>e.g. CREATE TABLE"]
        DDL --> HEAP["heap_create_with_catalog()<br/>src/backend/catalog/heap.c"]
        HEAP --> CTI["CatalogTupleInsert()<br/>indexing.c:233"]
        CTI --> HI["heap_insert (catalog row)"]
        CTI --> CIHT["CacheInvalidateHeapTuple()<br/>inval.c"]
        CIHT --> OUTBOX["TransInvalidationInfo<br/>(per-txn outbox)"]
        HEAP --> RCS["RelationCreateStorage()<br/>storage.c:121<br/>emits XLOG_SMGR_CREATE"]
        RCS --> PEND["pendingDeletes list<br/>(unlink on abort)"]
    end

    subgraph COMMIT["Commit path"]
        REC["RecordTransactionCommit()<br/>xact.c:1304"]
        REC --> XGCM["xactGetCommittedInvalidationMessages()<br/>inval.c:883"]
        XGCM --> XLI["XLogInsert(RM_XACT_ID,<br/>XLOG_XACT_COMMIT)"]
        REC --> TICT["TransactionIdCommitTree()<br/>+ TransactionTreeSetCommitTsData()"]
        TICT --> CLOG["CLOG: 2 bits/XID<br/>pg_xact"]
        TICT --> CTS["pg_commit_ts<br/>(if track_commit_timestamp)"]
        XLI --> WAL[("WAL stream")]
        REC --> ATEOX["AtEOXact_Inval()<br/>inval.c:1026"]
        REC --> SDP["smgrDoPendingDeletes()<br/>file unlink at commit"]
    end

    subgraph CHECKPOINT["Checkpoint"]
        CCP["CreateCheckPoint()<br/>xlog.c:6863"]
        CCP --> CCG["CheckPointGuts()<br/>xlog.c:7504"]
        CCG --> CCRMAP["CheckPointRelationMap"]
        CCG --> CCCLOG["CheckPointCLOG"]
        CCG --> CCCTS["CheckPointCommitTs"]
        CCG --> CCSUB["CheckPointSUBTRANS"]
        CCG --> CCMX["CheckPointMultiXact"]
        CCG --> CCBUF["CheckPointBuffers"]
        CCP --> UCF["UpdateControlFile()<br/>xlog.c:4514"]
    end

    subgraph STANDBY["Standby replay"]
        SRX["StartupXLOG()<br/>xlog.c:5384<br/>+ ReadControlFile()"]
        SRX --> RDISP["rmgr dispatch<br/>(rmgrlist.h)"]
        RDISP --> XR["xact_redo_commit()<br/>xact.c:6068"]
        XR --> SCLOG["TransactionIdCommitTree<br/>(replays CLOG bits)"]
        XR --> PCIM["ProcessCommitted<br/>InvalidationMessages()<br/>inval.c:962"]
        PCIM --> SINV[("Shared sinval ring")]
        XR --> SDP2["smgrDoPendingDeletes<br/>(unlink files)"]
        RDISP --> CLOGR["clog_redo()<br/>(zeropage / truncate)"]
        RDISP --> RMR["relmap_redo()"]
        RDISP --> SMGR["smgr_redo()"]
        RDISP --> H2V["heap_xlog_visible()<br/>VM bit-set"]
    end

    OUTBOX --> XGCM
    PEND --> SDP
    WAL --> CCP
    WAL --> SRX
    UCF --> SRX
    SINV --> CACHES["Per-backend<br/>catcache + relcache<br/>flush"]
```

(This is `diagrams/01_persistence_pipeline.mermaid`.)

## What lives where on disk

A miniature `$PGDATA` map showing which subsystem owns which directory
or file:

```
$PGDATA/
├── global/
│   ├── pg_control                 [pg_control: cluster anchor]
│   ├── pg_filenode.map            [relmapper: shared catalogs]
│   ├── pg_internal.init           [relcache shortcut: shared catalogs]
│   └── <relfilenode>              [physical files for the 11 shared catalogs]
├── base/<dboid>/
│   ├── pg_filenode.map            [relmapper: nailed local catalogs]
│   ├── pg_internal.init           [relcache shortcut: per-database]
│   └── <relfilenode>[_fsm|_vm]    [heap + FSM + VM forks for every relation]
├── pg_xact/<segno>                [CLOG]
├── pg_subtrans/<segno>            [SUBTRANS]
├── pg_multixact/
│   ├── offsets/<segno>            [MultiXact offsets]
│   └── members/<segno>            [MultiXact members]
├── pg_commit_ts/<segno>           [CommitTs (if track_commit_timestamp = on)]
├── pg_serial/<segno>              [SSI; volatile]
├── pg_notify/<segno>              [LISTEN/NOTIFY queue; volatile]
├── pg_wal/<segno>                 [WAL segments]
├── pg_twophase/<gid>              [2PC state files]
├── pg_logical/                    [logical replication]
├── pg_replslot/                   [replication slots]
├── pg_dynshmem/                   [dynamic shared memory]
├── pg_stat_tmp/                   [stat collector temp files]
└── pg_tblspc/<oid>                [tablespace symlinks]
```

Full annotation in [appendix_pgdata_layout.md](appendix_pgdata_layout.md).

## Three things every PostgreSQL backend does at startup

1. **Read pg_control** (`ReadControlFile`, `xlog.c:4298`).
   Validates magic + CRC + `pg_control_version` + `catalog_version_no`
   + architecture flags. Loads `ControlFileData` into shmem.
2. **Read pg_filenode.map** (`load_relmap_file`, `relmapper.c:765`).
   Validates magic + CRC. Without this, the relcache cannot find
   pg_class.
3. **Initialize the relcache in three phases**
   (`RelationCacheInitialize` / `Phase2` / `Phase3`,
   `relcache.c:4102` for Phase3). Phase 2 calls `formrdesc` for the
   four nailed catalogs; Phase 3 tries `pg_internal.init` and falls
   back to live catalog scans. After Phase 3, the backend can open
   any relation by OID.

After these three steps, the rest of the database becomes navigable.

## Glossary preview

A few terms used heavily in the rest of this document:

| Term         | Meaning                                                          |
|--------------|------------------------------------------------------------------|
| **catalog**  | A `pg_*` table holding cluster-level metadata.                    |
| **nailed**   | A catalog whose relcache descriptor is hard-coded in `formrdesc`. |
| **mapped**   | A catalog whose relfilenode lives in pg_filenode.map.             |
| **shared**   | A catalog with one physical file across all databases.            |
| **SLRU**     | Simple Least-Recently-Used cache with on-disk segment files.      |
| **CLOG**     | The transaction commit log (pg_xact, 2 bits/XID).                 |
| **VM**       | Visibility Map (per-heap-page all-visible/all-frozen bits).        |
| **FSM**      | Free Space Map (per-heap-page free-space hint).                    |
| **relmap**   | The Oid → relfilenode map for nailed/shared catalogs.              |
| **sinval**   | Shared invalidation message ring buffer.                           |
| **redo**     | The act of replaying a WAL record (also called "WAL replay").      |
| **FPI**      | Full-page image (a.k.a. full-page write).                          |
| **hint bit** | A flag on a heap tuple's infomask that's recomputable from CLOG.    |

Full definitions in [appendix_glossary.md](appendix_glossary.md).

---

[Up: index.md](index.md)  |  [Prev: 01 Executive Summary](01_executive_summary.md)  |  [Next: 03 Catalog Data Model and Bootstrap](03_catalog_data_model_and_bootstrap.md)
