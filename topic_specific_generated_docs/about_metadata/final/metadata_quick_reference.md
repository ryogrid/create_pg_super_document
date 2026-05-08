# Metadata Quick Reference

[Up: index.md](index.md)

A 3-page printable summary of the PostgreSQL Metadata subsystem.
For depth, see chapters [01](01_executive_summary.md)–[21](21_deep_dives.md).

## The four data domains

| Domain                | Storage                                              | Durability                | Chapter                                |
|-----------------------|------------------------------------------------------|---------------------------|----------------------------------------|
| **System catalogs**   | 63 heap relations under `pg_catalog`                  | Full WAL                  | [03](03_catalog_data_model_and_bootstrap.md), [18](18_catalog_inventory.md) |
| **Commit-log family** | SLRU directories (`pg_xact`, `pg_subtrans`, `pg_multixact`, `pg_commit_ts`) | WAL or rebuildable        | [08](08_slru_framework.md)–[12](12_multixact.md), [19](19_slru_users_catalog.md) |
| **Visibility Map**    | `VM_FORKNUM` per heap                                  | bit-set: WAL; bit-clear: implicit in heap WAL | [13](13_visibility_map.md) |
| **Free Space Map**    | `FSM_FORKNUM` per heap                                  | hint only                 | [14](14_free_space_map.md)             |

## The unifying spine

```
                ┌──────────────────────┐
                │  pg_control          │   atomic 512-byte file
                │  global/pg_control   │   ControlFileData + CheckPoint
                └────────┬─────────────┘
                         ↑
                         │ UpdateControlFile
                         │
                ┌────────┴─────────────┐
                │  CheckPointGuts      │   xlog.c:7504
                │  flush every domain  │   (relmap → CLOG → CommitTs →
                │  in dispatch order   │    SUBTRANS → MultiXact →
                └────────┬─────────────┘    Predicate → Buffers →
                         │                  SyncReq → TwoPhase)
                         │
                ┌────────┴─────────────┐
                │  WAL stream          │   rmgrlist.h dispatch
                │  pg_wal/<segno>      │   30 metadata record types
                └──────────────────────┘
                         ↓
                  StartupXLOG (xlog.c:5384)
                  → ReadControlFile
                  → set cursors
                  → Startup* hooks
                  → PerformWalRecovery
                  → Trim* hooks
                  → DB_IN_PRODUCTION
```

## Key APIs (one-liner each)

### Catalog modification

```c
void CatalogTupleInsert(Relation, HeapTuple);                      /* indexing.c:233 */
void CatalogTupleUpdate(Relation, ItemPointer, HeapTuple);         /* indexing.c:313 */
void CatalogTupleDelete(Relation, ItemPointer);                    /* indexing.c:365 */

Oid  heap_create_with_catalog(const char *relname, ...);           /* heap.c:1105 */
void heap_drop_with_catalog (Oid relid);                           /* heap.c:1767 */
Oid  index_create(Relation heap, ...);                             /* index.c:724 */

void recordDependencyOn(const ObjectAddress *dep,
                        const ObjectAddress *ref,
                        DependencyType behavior);                  /* pg_depend.c:46 */
void performDeletion(const ObjectAddress *obj,
                     DropBehavior, int flags);                     /* dependency.c:273 */

Oid  RangeVarGetRelid(const RangeVar *, LOCKMODE, bool missing_ok);/* macro -> namespace.c:441 */

SMgrRelation RelationCreateStorage(RelFileLocator, char relpersistence,
                                   bool register_delete);          /* storage.c:121 */
void log_smgrcreate(const RelFileLocator *, ForkNumber);            /* storage.c:186 */
void RelationDropStorage(Relation);                                /* storage.c:206 */
```

### Catalog reading (catcache → syscache → relcache)

```c
HeapTuple SearchSysCache1(int cacheId, Datum key1);                /* syscache.c:221 */
void      ReleaseSysCache(HeapTuple);                              /* syscache.c:269 */
Relation  RelationIdGetRelation(Oid);                              /* relcache.c:2063 */
void      RelationClose(Relation);                                 /* relcache.c:2194 */
```

### Cache invalidation

```c
void CacheInvalidateHeapTuple(Relation, HeapTuple, HeapTuple newtup); /* inval.c:1207 */
void AcceptInvalidationMessages(void);                                /* inval.c */
int  xactGetCommittedInvalidationMessages(SharedInvalidationMessage **msgs,
                                          bool *RelcacheInitFileInval); /* inval.c:883 */
void ProcessCommittedInvalidationMessages(...);                       /* inval.c:962 */
```

### Relmapper

```c
RelFileNumber RelationMapOidToFilenumber(Oid relid, bool shared);  /* relmapper.c:165 */
void          RelationMapUpdateMap(Oid, RelFileNumber, bool shared,
                                   bool immediate);                /* relmapper.c:325 */
void          relmap_redo(XLogReaderState *record);                /* relmapper.c:1096 */
```

### SLRU framework

```c
void SimpleLruInit(SlruCtl, const char *name, int nslots, int nlsns,
                   const char *subdir, int buffer_tranche, int bank_tranche,
                   SyncRequestHandler, bool long_segment_names);
int  SimpleLruReadPage  (SlruCtl, int64 pageno, bool write_ok, TransactionId xid); /* slru.c:502 */
void SimpleLruWritePage (SlruCtl, int slotno);                     /* slru.c:729 */
void SimpleLruWriteAll  (SlruCtl, bool allow_redirtied);            /* slru.c:1319 */
void SimpleLruTruncate  (SlruCtl, int64 cutoffPage);                /* slru.c:1405 */
```

### CLOG / CommitTs / MultiXact

```c
void      TransactionIdSetTreeStatus(TransactionId, int n_subxids,
                                     TransactionId *subxids,
                                     XidStatus status, XLogRecPtr lsn);  /* clog.c:183 */
XidStatus TransactionIdGetStatus    (TransactionId, XLogRecPtr *lsn);    /* clog.c:735 */

void      TransactionIdSetCommitTs   (TransactionId, TimestampTz, RepOriginId, int slotno); /* commit_ts.c:249 */
bool      TransactionIdGetCommitTsData(TransactionId, TimestampTz *ts, RepOriginId *nodeid); /* commit_ts.c:274 */

MultiXactId MultiXactIdCreate       (TransactionId xid1, MultiXactStatus,
                                     TransactionId xid2, MultiXactStatus); /* multixact.c:433 */
int         GetMultiXactIdMembers    (MultiXactId, MultiXactMember **,
                                      bool from_pgupgrade, bool isLockOnly); /* multixact.c:1293 */
```

### Visibility Map / Free Space Map

```c
void   visibilitymap_set        (Relation, BlockNumber heapBlk, Buffer heapBuf,
                                 XLogRecPtr recptr, Buffer vmBuf,
                                 TransactionId cutoff_xid, uint8 flags); /* vm.c:244 */
bool   visibilitymap_clear      (Relation, BlockNumber, Buffer vmbuf, uint8 flags); /* vm.c:138 */
uint8  visibilitymap_get_status (Relation, BlockNumber, Buffer *vmbuf); /* vm.c:336 */
void   visibilitymap_pin        (Relation, BlockNumber, Buffer *vmbuf); /* vm.c:191 */

BlockNumber GetPageWithFreeSpace          (Relation, Size needed);       /* freespace.c:137 */
void        RecordPageWithFreeSpace       (Relation, BlockNumber, Size avail); /* freespace.c:194 */
BlockNumber RecordAndGetPageWithFreeSpace (Relation, BlockNumber, Size avail, Size needed); /* freespace.c:154 */
void        FreeSpaceMapVacuum            (Relation);                    /* freespace.c:358 */
```

### Persistence

```c
void RecordTransactionCommit(void);                                /* xact.c:1304 */
void xact_redo_commit       (xl_xact_parsed_commit *, ...);        /* xact.c:6068 */
void xact_redo_abort        (xl_xact_parsed_abort  *, ...);        /* xact.c:6222 */
void CreateCheckPoint       (int flags);                           /* xlog.c:6863 */
void CheckPointGuts         (XLogRecPtr redo, int flags);          /* xlog.c:7504 */
void UpdateControlFile      (void);                                /* xlog.c:4514 */
void StartupXLOG            (void);                                /* xlog.c:5384 */
void ReadControlFile        (void);                                /* xlog.c:4298 */
```

## Checkpoint dispatch order (CheckPointGuts)

```
1. CheckPointRelationMap        relmapper.c
2. CheckPointReplicationSlots   replication
3. CheckPointSnapBuild          logical decoding
4. CheckPointLogicalRewriteHeap logical decoding
5. CheckPointReplicationOrigin  replication
6. CheckPointCLOG               clog.c   (SimpleLruWriteAll XactCtl)
7. CheckPointCommitTs           commit_ts.c
8. CheckPointSUBTRANS           subtrans.c
9. CheckPointMultiXact          multixact.c (offsets + members + pg_control)
10. CheckPointPredicate         predicate.c (Serial SLRU)
11. CheckPointBuffers           bufmgr.c   (every dirty page)
12. ProcessSyncRequests         sync.c     (fsync queue)
13. CheckPointTwoPhase          twophase.c
14. UpdateControlFile           xlog.c
```

## Recovery sequence (StartupXLOG)

```
1. ReadControlFile()
2. Validate magic + CRC + pg_control_version + catalog_version_no + arch flags
3. Determine recovery mode (DB_SHUTDOWNED → no replay; else replay)
4. Set ShmemVariableCache cursors from CheckPointCopy
5. StartupCLOG, StartupCommitTs, StartupSUBTRANS, StartupMultiXact
6. PerformWalRecovery — replay WAL from CheckPointCopy.redo
7. TrimCLOG, TrimMultiXact (zero trailing portions of live pages)
8. state = DB_IN_PRODUCTION; UpdateControlFile
9. Signal Postmaster — open for connections
```

## Key GUCs

| GUC                                | Default | Why it matters                                                |
|------------------------------------|---------|---------------------------------------------------------------|
| `track_commit_timestamp`           | off     | Activates CommitTs SLRU.                                      |
| `wal_log_hints`                    | off     | FPI for hint-bit-only changes (req'd under data checksums).   |
| `synchronous_commit`               | on      | Off enables async commit; CLOG group_lsn becomes critical.    |
| `autovacuum_freeze_max_age`        | 200M    | XID-wraparound emergency threshold.                            |
| `autovacuum_multixact_freeze_max_age` | 400M | MultiXactId-wraparound threshold.                              |
| `transaction_buffers`              | -1      | CLOG slot count (auto-tune by default).                        |
| `multixact_member_buffers`         | -1      | MultiXact members slot count.                                  |
| `commit_timestamp_buffers`         | -1      | CommitTs slot count.                                            |
| `checkpoint_timeout`               | 5min    | Time between checkpoints.                                      |
| `max_wal_size`                     | 1GB     | WAL-size-driven checkpoint trigger.                            |
| `wal_level`                        | replica | minimal / replica / logical.                                    |

Full GUC list in [appendix_guc_parameters.md](appendix_guc_parameters.md).

## Diagnostic SQL

```sql
-- visibility map: how many heap pages are all-visible / all-frozen
SELECT * FROM pg_visibility_map_summary('mytable'::regclass);

-- free space map per page
SELECT blkno, avail FROM pg_freespace('mytable'::regclass);

-- transaction commit status
SELECT pg_xact_status(123456);

-- multi-xact members (when xmax is a multi)
SELECT * FROM pg_get_multixact_members(456);

-- catalog cache hit rate (per-cache)
SELECT * FROM pg_stat_slru;

-- SLRU stats (since PG 13)
SELECT name, blks_zeroed, blks_hit, blks_read, blks_written, blks_exists,
       flushes, truncates FROM pg_stat_slru;

-- Look up an Oid → object name
SELECT oid::regclass FROM pg_class WHERE oid = 1259;     -- pg_class itself

-- Last committed transaction (requires track_commit_timestamp)
SELECT pg_last_committed_xact();
SELECT pg_xact_commit_timestamp(123456);
```

---

[Up: index.md](index.md)
