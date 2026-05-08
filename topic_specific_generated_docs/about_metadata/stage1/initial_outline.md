# PostgreSQL Metadata Subsystem — Documentation Outline

This outline drives the Stage-2 narrative. Estimated total length: **~9,000 lines**
(~280 KB plain text), comparable in scope to the existing planner topic doc.

The narrative builds the metadata subsystem from the bottom up:
the on-disk anchor (`pg_control`) → the SLRU framework → CLOG / SUBTRANS /
MultiXact / CommitTs → relmapper → system catalogs → catalog modification
APIs → catalog-cache stack → invalidation broadcast → VM/FSM → the integrating
"persistence guarantees" story → checkpoint/recovery sequencing.

Each section lists its priority and an estimated line count. **Critical** sections
must include line-anchored source references; **Important** sections must include
prose for every public symbol they touch; **Supporting** sections may rely on
links from the Critical and Important sections.

---

## 1. The cluster anchor — `pg_control` and the recovery contract — 400 lines (Critical)

- 1.1 What is durability? The WAL-before-data rule
  (refer to `src/backend/access/transam/README` lines 399–435)
- 1.2 `ControlFileData` — the schema (pg_control.h:104) and what each field anchors
  - `system_identifier`, `pg_control_version`, `catalog_version_no`
  - `state` (DBState enum), `time`, `checkPoint`, `checkPointCopy`
  - `unloggedLSN`, `minRecoveryPoint`, `backupStartPoint/EndPoint/EndRequired`
  - parameter snapshot (`wal_level`, `wal_log_hints`, `MaxConnections`,
    `track_commit_timestamp`, …)
  - architecture compatibility (maxAlign, floatFormat, blcksz, relseg_size,
    xlog_blcksz, xlog_seg_size, nameDataLen, indexMaxKeys,
    toast_max_chunk_size, loblksize, float8ByVal, data_checksum_version)
  - `mock_authentication_nonce`, CRC
- 1.3 The metadata cursors carried in `CheckPoint` (pg_control.h:35)
  - `redo`, `nextXid`, `nextOid`, `nextMulti`, `nextMultiOffset`, `oldestXid`,
    `oldestMulti`, `oldestCommitTsXid`, `newestCommitTsXid`, `oldestActiveXid`
- 1.4 Lifecycle: `WriteControlFile`, `ReadControlFile`, `UpdateControlFile`
  (xlog.c:4216 area). Why pg_control must fit in `PG_CONTROL_MAX_SAFE_SIZE` (512).

## 2. The SLRU framework — 600 lines (Critical)

- 2.1 What is an SLRU and why do we need it?
  - the difference between a heap fork and an SLRU
  - segment files, PAGES_PER_SEGMENT, hex segment names
  - bank-partitioned page lookup; SLRU_BANK_BITSHIFT
- 2.2 `SlruCtlData` and `SlruSharedData` (slru.h:61, 127)
  - page slots, page_status (`SLRU_PAGE_EMPTY/READ_IN_PROGRESS/VALID/WRITE_IN_PROGRESS`)
  - `bank_locks`, `buffer_locks`, `latest_page_number` (pg_atomic_uint64)
  - the `group_lsn[]` array (only used by CLOG)
- 2.3 The eight public APIs
  - `SimpleLruInit`, `SimpleLruZeroPage`, `SimpleLruReadPage`,
    `SimpleLruReadPage_ReadOnly`, `SimpleLruWritePage`, `SimpleLruWriteAll`,
    `SimpleLruTruncate`, `SlruScanDirectory`
- 2.4 The replacement algorithm
  - per-bank LRU, `SlruSelectLRUPage`, dirty-write-then-evict
  - `SimpleLruWaitIO` for in-progress reads/writes
- 2.5 The sync-request integration
  - `SyncRequestHandler`, `SlruSyncFileTag`, the four per-SLRU
    `*syncfiletag` callbacks; how the checkpointer fsyncs SLRU segments

## 3. CLOG — transaction commit/abort log — 600 lines (Critical)

- 3.1 The data model: 2 bits per XID; XidStatus values (`TRANSACTION_STATUS_IN_PROGRESS`,
  `_COMMITTED`, `_ABORTED`, `_SUB_COMMITTED`)
- 3.2 The page address arithmetic (`TransactionIdToPage`, `_ToPgIndex`, `_ToByte`,
  `_ToBIndex`); `CLOG_XACTS_PER_PAGE = BLCKSZ * 4`
- 3.3 Setting status — the four-layer stack
  - `TransactionIdSetTreeStatus` → `TransactionIdSetPageStatus` →
    `TransactionIdSetPageStatusInternal` → `TransactionIdSetStatusBit`
  - the multi-page tree case (sub-committed protocol)
  - the group-commit batching: `TransactionGroupUpdateXidStatus`
- 3.4 Reading status — `TransactionIdGetStatus` (clog.c:735)
  - LSN feedback for asynchronous commit
  - `CLOG_XACTS_PER_LSN_GROUP = 32`, `CLOG_LSNS_PER_PAGE`, `GetLSNIndex`
  - the contract: a hint bit may not be written ahead of the WAL flush
- 3.5 Lifecycle
  - `BootStrapCLOG`, `StartupCLOG`, `TrimCLOG`, `ExtendCLOG`,
    `TruncateCLOG`, `CheckPointCLOG`
- 3.6 WAL records
  - `XLOG_CLOG_ZEROPAGE` (CLOG_ZEROPAGE = 0x00)
  - `XLOG_CLOG_TRUNCATE` (CLOG_TRUNCATE = 0x10) carrying `xl_clog_truncate`
  - `clog_redo` dispatcher
- 3.7 Why `XLOG_XACT_COMMIT` does not need its own CLOG record
  (clog.c top comment, lines 14–25)

## 4. SUBTRANS — subtransaction parent chain — 200 lines (Important)

- 4.1 Data model: 4 bytes per XID (parent TransactionId)
- 4.2 The "no WAL" decision; how recovery rebuilds the file
- 4.3 `SubTransSetParent`, `SubTransGetParent`, `SubTransGetTopmostTransaction`
- 4.4 `StartupSUBTRANS` zero-fill behavior; truncation under
  `vac_truncate_clog`
- 4.5 Why this is the simplest SLRU and what we can learn from it

## 5. CommitTs — commit timestamps — 250 lines (Important)

- 5.1 Data model: 10 bytes per XID (TimestampTz + RepOriginId)
- 5.2 `track_commit_timestamp` GUC and the activate/deactivate dance
  (`CommitTsParameterChange`, `ActivateCommitTs`, `DeactivateCommitTs`)
- 5.3 `TransactionTreeSetCommitTsData` and the (committssyncfiletag,
  commit_ts_redo) wiring
- 5.4 WAL: `XLOG_COMMIT_TS_ZEROPAGE`, `XLOG_COMMIT_TS_TRUNCATE`
- 5.5 SQL exposure: `pg_xact_commit_timestamp`, `pg_last_committed_xact`,
  `pg_xact_commit_timestamp_origin`

## 6. MultiXact — multi-XID locks for shared row locks — 600 lines (Critical)

- 6.1 What problem does MultiXact solve? FOR KEY SHARE / FOR SHARE / FOR UPDATE
  lock combination on the same tuple
- 6.2 The two-SLRU split: `pg_multixact/offsets` and `pg_multixact/members`
  - offsets: MultiXactId → first member offset (4 bytes per multi)
  - members: variable-length array of `MultiXactMember` structs
    (TransactionId + MultiXactStatus packed via flag-byte groups)
- 6.3 The `MultiXactStatus` enum (multixact.h:37) and ISUPDATE_from_mxstatus
- 6.4 Construction
  - `MultiXactIdCreate`, `MultiXactIdExpand`, `MultiXactIdCreateFromMembers`
  - `GetNewMultiXactId`, `RecordNewMultiXact`
  - the page-zeroing dance for both SLRUs
- 6.5 Reading
  - `GetMultiXactIdMembers`, `MultiXactIdIsRunning`,
    `MultiXactIdGetUpdateXid`, `mXactCacheGetById`
- 6.6 Wraparound concerns
  - `SetOffsetVacuumLimit`, `MultiXactMemberFreezeThreshold`,
    `MultiXactAdvanceOldest`
  - the `oldestMulti`, `oldestMultiDB` cursors in pg_control
- 6.7 WAL records
  - `XLOG_MULTIXACT_ZERO_OFF_PAGE`, `_ZERO_MEM_PAGE`, `_CREATE_ID`, `_TRUNCATE_ID`
  - the `xl_multixact_create` and `xl_multixact_truncate` payloads
- 6.8 2PC support: `multixact_twophase_recover`, `_postcommit`, `_postabort`

## 7. relmapper — the metadata-of-the-metadata — 400 lines (Critical)

- 7.1 Why pg_class.relfilenode does not work for nailed and shared catalogs
  (relmapper.c top comment, lines 6–29)
- 7.2 `RelMapping`, `RelMapFile`, `RELMAPPER_FILEMAGIC = 0x592717`
- 7.3 The active / pending update pattern; CCI semantics
- 7.4 `RelationMapOidToFilenumber` — the lookup path; how `formrdesc`
  uses it during relcache bootstrap
- 7.5 `RelationMapUpdateMap` — the write path; commit-time
  `perform_relmap_update` → `write_relmap_file` (atomic temp+fsync+rename+fsync)
- 7.6 `XLOG_RELMAP_UPDATE` and `relmap_redo` — how a standby catches up
- 7.7 Why mapped relations cannot be relocated by transactional commands
  (only VACUUM FULL / CLUSTER, which write the new file before commit)
- 7.8 `CheckPointRelationMap` — the lock that guards us against a half-applied
  rename mid-checkpoint
- 7.9 Initialization phases (Phase 1 / 2 / 3) and how they interleave with
  relcache initialization

## 8. The system catalog data model — 800 lines (Critical)

- 8.1 The `CATALOG()` macro and the BKI flags
  (`BKI_BOOTSTRAP`, `BKI_SHARED_RELATION`, `BKI_ROWTYPE_OID`, `BKI_SCHEMA_MACRO`)
- 8.2 The 11 shared catalogs (one row from `IsSharedRelation`)
- 8.3 The 4 nailed catalogs and how `formrdesc` builds them
- 8.4 The 48 ordinary catalogs — the inventory in `catalog_inventory.txt`
- 8.5 `DECLARE_INDEX` / `DECLARE_UNIQUE_INDEX` / `DECLARE_UNIQUE_INDEX_PKEY`
  in the catalog headers — how every system index is declared
- 8.6 The `.dat` format and its role in bootstrap
- 8.7 Bootstrap: `genbki.pl` → `postgres.bki` → `bootstrap.c` /
  `bootparse.y` / `bootscanner.l`
- 8.8 `catversion.h` (`CATALOG_VERSION_NO`) and the on-disk compatibility check
- 8.9 Per-catalog C-side helpers (`pg_proc.c`, `pg_type.c`, etc.) — what
  belongs in a helper vs in `heap.c`/`index.c`

## 9. Catalog modification APIs — 700 lines (Critical)

- 9.1 The "sanctioned mutators" rule: only via `CatalogTuple*`
  (indexing.c:233, 256, 273, 313, 337, 365)
- 9.2 `heap.c` — `heap_create_with_catalog`, `heap_create`, `heap_drop_with_catalog`,
  `AddNewRelationTuple`, `AddNewAttributeTuples`, `InsertPgClassTuple`,
  `RelationRemoveInheritance`, `CheckAttributeType`
- 9.3 `index.c` — `index_create`, `index_drop`, `index_constraint_create`,
  `index_update_stats`, `IndexSetParentIndex`, `ReindexRelationConcurrently`
- 9.4 `dependency.c` — `recordDependencyOn`, `performDeletion`,
  `findDependentObjects`, `doDeletion`; the dependency graph walking algorithm
- 9.5 `namespace.c` — `RangeVarGetRelid`, `RangeVarGetCreationNamespace`,
  `LookupExplicitNamespace`, `recomputeNamespacePath`; the search_path cache
- 9.6 `storage.c` — the bridge from catalog to physical:
  `RelationCreateStorage`, `RelationDropStorage`, `log_smgrcreate`,
  `smgrDoPendingDeletes`, `smgrDoPendingSyncs`, `RelationTruncate`
- 9.7 `toasting.c` — `NewHeapCreateToastTable`
- 9.8 `aclchk.c` — `ExecGrantStmt_oids`, `pg_class_aclmask`,
  `pg_namespace_aclmask`
- 9.9 `objectaddress.c` — `get_object_address` and how DDL machinery
  uses it
- 9.10 `objectaccess.c` — the hook surface for sepgsql / pg_audit
- 9.11 `partition.c` and `pg_inherits.c`
- 9.12 The "in-place update" exception
  - `IsInplaceUpdateRelation` (pg_class, pg_database) and
    `heap_inplace_update_and_unlock`
  - why VACUUM updates pg_class.relfrozenxid in place

## 10. The catalog-cache stack — 900 lines (Critical)

- 10.1 The three caches: catcache (per-key), relcache (per-relation),
  plancache (per-prepared-plan). Why three layers?
- 10.2 `catcache.c` deep-dive
  - `CatCache` struct, hash buckets, `CatCTup`, `CatCList`
  - `InitCatCache`, `CatalogCacheInitializeCache` (lazy)
  - `SearchCatCacheInternal` hot path; `SearchCatCacheMiss` cold path
  - negative entries (`CT_NEGATIVE`)
  - `CatCacheRemoveCTup`, `CatCacheRemoveCList`, `CatCacheInvalidate`
  - the cacheinfo[] table generated by genbki.pl into syscache_info.h
- 10.3 `syscache.c` API surface
  - the cacheinfo[] table; `SysCacheIdentifier` enum (generated)
  - `SearchSysCache1..4`, `ReleaseSysCache`, `SearchSysCacheCopy*`
  - `SearchSysCacheLocked1` — the row-lock variant (syscache.c:287)
  - `SearchSysCacheAttName`, `_AttNum`
  - `SysCacheGetAttr`, `_AttrNotNull`
  - `GetSysCacheOid`, `GetSysCacheHashValue`, `SysCacheInvalidate`
  - `RelationInvalidatesSnapshotsOnly`, `RelationHasSysCache`,
    `RelationSupportsSysCache`
- 10.4 `relcache.c` deep-dive
  - `RelationData` field-by-field tour (rel.h)
  - `RelationIdGetRelation` → `RelationBuildDesc` →
    `ScanPgRelation` → `AllocateRelationDesc` → `RelationBuildTupleDesc` →
    `RelationBuildRuleLock` → `RelationInitPhysicalAddr` →
    `RelationInitTableAccessMethod` / `RelationInitIndexAccessInfo`
  - the three phases of relcache init (`RelationCacheInitialize`, `Phase2`, `Phase3`)
  - `formrdesc` and the hard-coded descriptors for nailed catalogs
  - `load_critical_index` for the indexes that nailed catalogs depend on
  - the `pg_internal.init` cache: `write_relcache_init_file`,
    `load_relcache_init_file`, `RelationCacheInitFileRemove`
  - `RelationClearRelation` — the rebuild-vs-destroy decision tree
  - `RelationFlushRelation`, `RelationCacheInvalidate`,
    `RelationCacheInvalidateEntry`
  - `RelationSetNewRelfilenumber` and the relmapper interaction
- 10.5 `plancache.c`, `partcache.c`, `typcache.c`, `evtcache.c`,
  `attoptcache.c`, `spccache.c`, `ts_cache.c`, `relfilenumbermap.c`
  — short tour of the auxiliary caches
- 10.6 The lifecycle of one relcache entry from build to invalidation

## 11. Cache invalidation — keeping caches consistent — 700 lines (Critical)

- 11.1 The contract: catalog mutators emit messages; consumers process them
  at well-defined points (CCI, transaction end, `AcceptInvalidationMessages`)
- 11.2 The per-transaction outbox in `inval.c`
  - `TransInvalidationInfo` and the message-group split
  - `PrepareInvalidationState`
  - `AddCatcacheInvalidationMessage`, `AddRelcacheInvalidationMessage`,
    `AddSnapshotInvalidationMessage`
- 11.3 Mutator-side hooks
  - `CacheInvalidateHeapTuple` (the central trigger from `CatalogTuple*`)
  - `CacheInvalidateHeapTupleByRelid`, `CacheInvalidateRelcache`,
    `CacheInvalidateRelcacheAll`, `CacheInvalidateRelcacheByRelid`,
    `CacheInvalidateCatalog`, `CacheInvalidateSmgr`, `CacheInvalidateRelmap`
- 11.4 Commit-time emission
  - `xactGetCommittedInvalidationMessages` packs the outbox into
    `xl_xact_commit`
  - `RecordTransactionCommit` inserts the WAL record then calls
    `AtEOXact_Inval`
- 11.5 Local replay (`AtEOXact_Inval`, `AtEOSubXact_Inval`,
  `CommandEndInvalidationMessages`, `LocalExecuteInvalidationMessage`)
- 11.6 Remote replay
  - `ProcessCommittedInvalidationMessages` on standbys
  - `SendSharedInvalidMessages` push to ring buffer
- 11.7 The shared ring buffer (`sinval.c`, `sinvaladt.c`)
  - `SharedInvalidStateData`, per-backend cursor, write-lock, read-lock
  - `SIInsertDataEntries`, `SIGetDataEntries`, `SICleanupQueue`
  - overflow → `SI_RESET` → every backend wipes all caches
  - `HandleCatchupInterrupt`, `ProcessCatchupInterrupt`
- 11.8 The on-arrival path
  - `AcceptInvalidationMessages` (called from `LockRelationOid`,
    `RelationIdGetRelation`, etc.)
  - `ReceiveSharedInvalidMessages` -> `LocalExecuteInvalidationMessage`
- 11.9 Extension hooks
  - `CacheRegisterSyscacheCallback`, `CacheRegisterRelcacheCallback`
  - `CallSyscacheCallbacks`, `CallRelcacheCallbacks`

## 12. Visibility Map — page-level all-visible / all-frozen bits — 500 lines (Critical)

- 12.1 The data model: 2 bits per heap page in the VM_FORKNUM relation fork.
  `VISIBILITYMAP_ALL_VISIBLE = 0x01`, `VISIBILITYMAP_ALL_FROZEN = 0x02`
  (visibilitymapdefs.h)
- 12.2 The size: `HEAPBLOCKS_PER_BYTE = 4`,
  `HEAPBLOCKS_PER_PAGE = (BLCKSZ - SizeOfPageHeaderData) * 4`
- 12.3 The bit-clear contract — durably maintained
  - `visibilitymap_clear` is called by every heap mutation that breaks
    all-visible: `heap_insert`, `heap_update`, `heap_delete`,
    `heap_lock_tuple`, `heap_multi_insert`
  - the clearing is implicit in the heap WAL record's redo
- 12.4 The bit-set contract — a high-water-mark protocol
  - `visibilitymap_set` takes the heap-page LSN, ensuring the VM page LSN
    is at least as new as the youngest tuple it claims visible
  - `XLOG_HEAP2_VISIBLE` and `xl_heap_visible`
  - the relationship to PD_ALL_VISIBLE (the per-heap-page hint bit)
- 12.5 The pin-before-lock deadlock-avoidance protocol
  - `visibilitymap_pin`, `visibilitymap_pin_ok`
  - `GetVisibilityMapPins` (hio.c) for multi-block insert
- 12.6 Read paths
  - `visibilitymap_get_status` — used by index-only scans (skip heap fetch)
    and by VACUUM (decide if scan needed)
  - `visibilitymap_count` — counts ALL_VISIBLE / ALL_FROZEN pages
- 12.7 Truncation
  - `visibilitymap_prepare_truncate` from `RelationTruncate`
- 12.8 Storage details
  - `vm_readbuf`, `vm_extend`, the VM relation fork
  - why VM page writes do not normally need full-page images

## 13. Free Space Map — fast page-with-space lookup — 500 lines (Critical)

- 13.1 The data model: 1 byte per heap page (256 categories of free space)
  in the FSM_FORKNUM relation fork
- 13.2 The two-level tree
  - per-page binary heap (fsmpage.c) with `fsm_search_avail` /
    `fsm_set_avail` / `fsm_rebuild_page`
  - across-page tree (freespace.c) with `fsm_get_location`,
    `fsm_logical_to_physical`, `fsm_get_parent`, `fsm_get_child`
  - the closed-form formula for physical block #
    `y = n + (n/F + 1) + (n/F^2 + 1) + ... + 1`
- 13.3 Search
  - `GetPageWithFreeSpace` → `fsm_search` → top-down walk
  - `fsm_search_avail` selects the leftmost slot ≥ minvalue
  - `fp_next_slot` for spreading inserts across pages
- 13.4 Update
  - `RecordPageWithFreeSpace`, `RecordAndGetPageWithFreeSpace`
  - `fsm_set_and_search` — combined update + next-page lookup
- 13.5 Vacuum reconciliation
  - `FreeSpaceMapVacuum`, `FreeSpaceMapVacuumRange`, `fsm_vacuum_page`
- 13.6 Categories: `fsm_space_avail_to_cat` and the 256-category table
- 13.7 The "FSM is just a hint" contract
  - `MarkBufferDirtyHint`, `RBM_ZERO_ON_ERROR`, no WAL emission for ordinary
    FSM updates
  - `XLogRecordPageWithFreeSpace` for the heap-extension special case
  - `fsm_does_block_exist` — checking the FSM hint against actual relation size
- 13.8 hio.c integration: `RelationGetBufferForTuple`
- 13.9 indexfsm.c — the simpler 0/1 FSM for indexes:
  `GetFreeIndexPage`, `RecordFreeIndexPage`, `RecordUsedIndexPage`,
  `IndexFreeSpaceMapVacuum`

## 14. The persistence integration story — 700 lines (Critical)

- 14.1 The four durability strategies for metadata
  - (a) WAL-logged changes (CLOG, CommitTs, MultiXact, RelMap, XACT)
  - (b) Reconstructable from runtime state (SUBTRANS, sinval queue)
  - (c) Hint structures rebuilt on demand (FSM, parts of VM)
  - (d) Structures embedded in WAL records or in pg_control
- 14.2 The buffer manager as the synchronization boundary
  - dirty-then-flush ordering
  - the LSN check before page write
  - hint bits and `MarkBufferDirtyHint`
- 14.3 The WAL flush boundary
  - `XLogFlush` semantics
  - how CLOG's group-LSN array enforces ordering
- 14.4 The transaction commit pathway in detail
  - `RecordTransactionCommit` build of `xl_xact_commit`
  - `XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT)`
  - `TransactionIdCommitTree` → CLOG update
  - `AtEOXact_Inval` → sinval broadcast
  - `smgrDoPendingDeletes` → file unlink
  - `AtEOXact_RelationMap` → `perform_relmap_update`
- 14.5 The redo pathway in detail (`xact_redo_commit`)
  - `TransactionIdCommitTree`
  - `ProcessCommittedInvalidationMessages`
  - `smgrDoPendingDeletes`
- 14.6 Checkpoint sequencing — `CheckPointGuts` (xlog.c:7504)
  - `CheckPointRelationMap`
  - `CheckPointReplicationSlots`, `CheckPointSnapBuild`,
    `CheckPointLogicalRewriteHeap`, `CheckPointReplicationOrigin`
  - `CheckPointCLOG`, `CheckPointCommitTs`, `CheckPointSUBTRANS`,
    `CheckPointMultiXact`, `CheckPointPredicate`
  - `CheckPointBuffers`
  - `ProcessSyncRequests`
  - `CheckPointTwoPhase`
  - `UpdateControlFile`
- 14.7 Startup sequencing — `StartupXLOG` (xlog.c:5384)
  - `ReadControlFile`
  - restore checkpoint pointer; set XID/Multi/CommitTs cursors
  - `StartupCLOG`, `StartupSUBTRANS`, `StartupMultiXact`, `StartupCommitTs`
  - replay WAL from redo to end (`PerformWalRecovery`)
  - `TrimCLOG`, `TrimMultiXact`
  - mark consistent; open for connections

## 15. WAL records that touch metadata — 350 lines (Important)

- 15.1 The `rmgrlist.h` master list — what each RM_* is for
- 15.2 The 30 distinct metadata-affecting WAL records
  (full inventory in `wal_record_inventory.txt`)
- 15.3 Implicit metadata effects
  - heap mutating records implicitly clear VM bits
  - `XLOG_XACT_COMMIT`/`_ABORT` implicitly update CLOG via the redo function
  - hint bits and `XLOG_FPI_FOR_HINT`

## 16. Hooks and extension points — 200 lines (Supporting)

- 16.1 `object_access_hook` (objectaccess.c) — sepgsql, pg_audit
- 16.2 Catcache callback registry (`CacheRegisterSyscacheCallback`,
  `CacheRegisterRelcacheCallback`)
- 16.3 The other extension surfaces around metadata: `ExecutorStart_hook`,
  `planner_hook`, `process_utility_hook` interactions

## 17. Reading pg_filenode.map and pg_control with `pg_controldata` — 100 lines (Supporting)

- 17.1 `src/bin/pg_controldata` walkthrough
- 17.2 Diagnostic reading of pg_filenode.map (no public tool, but the format
  is documented in relmapper.c)

---

## Suggested supplementary appendices (~600 lines total)

- A. Full catalog inventory cross-reference (catalog.h ↔ indexing decls ↔ .dat
     ↔ pg_*.c helper). Source: `catalog_inventory.txt`.
- B. Full SLRU inventory (`slru_inventory.txt`).
- C. Full WAL record inventory (`wal_record_inventory.txt`).
- D. Glossary: nailed catalog, mapped catalog, sinval reset, group commit,
  CCI, MultiXact, etc.

---

## Stage-2 plan summary

Approximate priority allocations:

| Category               | Lines  |
|------------------------|--------|
| pg_control / recovery  |   400  |
| SLRU framework + 4 inst| 2,250  |
| relmapper              |   400  |
| Catalog data model     |   800  |
| Catalog mutation API   |   700  |
| Catalog cache stack    |   900  |
| Invalidation broadcast |   700  |
| VM                     |   500  |
| FSM                    |   500  |
| Persistence story      |   700  |
| WAL record inventory   |   350  |
| Hooks                  |   200  |
| Diagnostics            |   100  |
| Appendices             |   600  |
| **TOTAL**              | **~9,100** |

Each section will follow the planner doc's pattern of:
- short narrative introduction (10-30 lines)
- file:line references for every public symbol mentioned
- pseudo-code or flowchart for non-trivial control flow
- pointers to the inventory tables for completeness
