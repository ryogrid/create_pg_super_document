# PostgreSQL Metadata Subsystem Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's **Metadata** subsystem — the on-disk and in-memory machinery that records *what objects exist*, *what state every transaction is in*, *which heap pages are visible / frozen*, and *which heap pages have free space* — and **how the persistence of every one of these data structures is guaranteed across crashes, checkpoints, and replication**. The documentation must cover four primary metadata domains and the unifying persistence story that ties them together:

1. **System catalogs (`pg_catalog`)** — the data dictionary itself. The bootstrap process (`genbki.pl` → `postgres.bki`) that materializes initial catalog rows from `.dat` files, the C-side infrastructure that creates / mutates / drops catalog rows (`catalog/heap.c`, `catalog/index.c`, `catalog/indexing.c`, `catalog/dependency.c`, `catalog/namespace.c`, `catalog/partition.c`, `catalog/storage.c`, `catalog/toasting.c`, `catalog/aclchk.c`, `catalog/objectaddress.c`, plus the per-catalog helpers `catalog/pg_*.c`), the catalog cache stack (`utils/cache/catcache.c`, `syscache.c`, `relcache.c`, `plancache.c`, `evtcache.c`, `partcache.c`, `typcache.c`, `attoptcache.c`, `spccache.c`, `ts_cache.c`, `relfilenumbermap.c`), the relmapper (`utils/cache/relmapper.c`) that lets nailed shared catalogs change their relfilenumber atomically, and the cache invalidation layer (`utils/cache/inval.c`, `storage/ipc/sinval.c`, `storage/ipc/sinvaladt.c`).
2. **Commit Log family — SLRU-backed transaction metadata** — `access/transam/slru.c` (the shared, fixed-slot, on-disk segmented buffer pool used by *all* of the following), `access/transam/clog.c` (commit/abort/sub-committed status per `TransactionId`), `access/transam/subtrans.c` (subtransaction → top-level XID linkage), `access/transam/commit_ts.c` (commit timestamps and replication-origin per XID), and `access/transam/multixact.c` (multi-transaction membership for shared row locks — split into `MultiXactOffset` SLRU and `MultiXactMember` SLRU).
3. **Visibility Map (VM)** — `access/heap/visibilitymap.c`, `include/access/visibilitymap.h`, `include/access/visibilitymapdefs.h`, with bits `VISIBILITYMAP_ALL_VISIBLE` and `VISIBILITYMAP_ALL_FROZEN`. Covers the bit-level semantics, the relationship with index-only scans (`heapam_visibility.c`, `genam.c`), the bit-clearing protocol on heap mutation (`heap_insert`, `heap_update`, `heap_delete`, `heap_multi_insert`, `heap_lock_tuple`, `heap_xlog_*`), and the bit-setting protocol in `vacuumlazy.c`.
4. **Free Space Map (FSM)** — `storage/freespace/freespace.c`, `storage/freespace/fsmpage.c`, `storage/freespace/indexfsm.c`, with the three-level (root / midlevel / leaf) tree, the `FSMAddress` arithmetic (`fsm_logical_to_physical`, `fsm_get_location`, `fsm_get_parent`, `fsm_get_child`), the page-internal binary heap (`fsmpage.c`'s `fsm_search_avail`, `fsm_set_avail`), the heap-extension fast path in `access/heap/hio.c` (`RelationGetBufferForTuple`, `GetVisibilityMapPins`), and the index FSM (`indexfsm.c`).

The documentation must additionally include a unified, system-wide treatment of:

5. **Persistence guarantees** — the single thread that ties all four domains together. **WAL is the durability boundary, the buffer manager + checkpointer is the synchronization boundary, and `pg_control` is the recovery anchor.** This section enumerates every WAL record that touches metadata (`RM_CLOG_ID`, `RM_MULTIXACT_ID`, `RM_RELMAP_ID`, `RM_COMMIT_TS_ID`, `RM_SMGR_ID` for relfilenode lifecycle, `RM_HEAP2_ID`'s `XLOG_HEAP2_VISIBLE` for the VM, and the FSM's special "non-WAL-logged but crash-safe through full-page images" approach), the SLRU page-flush + fsync protocol, the relmapper's atomic rename + fsync protocol, the catalog row's reliance on regular heap WAL via `XLogInsert`, the checkpoint hooks (`CheckPointCLOG`, `CheckPointSUBTRANS`, `CheckPointMultiXact`, `CheckPointCommitTs`, `CheckPointRelationMap`), the startup sequence (`StartupCLOG`, `StartupSUBTRANS`, `StartupMultiXact`, `TrimCLOG`, `TrimMultiXact`, `RelationMapInitialize*`), and the cross-replica propagation story (sinval through WAL on the standby).

The documentation must also produce **three systematic catalogs**:

- **System Catalog Inventory** — every table under `pg_catalog`, with: `Oid`, header file in `src/include/catalog/`, `.dat` initialization data file (when present), key indexes (from `indexing.h`), C-side modification helper (e.g., `pg_aggregate.c`, `pg_proc.c`), purpose, dependency relationships (which other catalogs reference its rows), and whether it is shared / nailed / mapped.
- **SLRU Users Catalog** — every SLRU-backed metadata file (CLOG, MultiXactOffset, MultiXactMember, SUBTRANS, CommitTs, the asynchronous notification queue `pg_notify` via `async.c`, and the predicate-locking serial log `predicate.c`), with: `SlruCtl` pointer, on-disk segment directory (`pg_xact`, `pg_multixact/offsets`, `pg_multixact/members`, `pg_subtrans`, `pg_commit_ts`, `pg_notify`, `pg_serial`), per-page layout, page-number formula for the corresponding key (XID, MultiXactId, etc.), bank-lock partitioning, retention / truncation policy, redo callback, and checkpoint hook.
- **Metadata WAL Record Catalog** — every WAL record affecting metadata: `XLOG_NEXTOID`, `XLOG_CHECKPOINT_*`, `XLOG_FPI`, `XLOG_FPI_FOR_HINT`, the CLOG records (`CLOG_ZEROPAGE`, `CLOG_TRUNCATE`), the MultiXact records (`XLOG_MULTIXACT_ZERO_OFF_PAGE`, `XLOG_MULTIXACT_ZERO_MEM_PAGE`, `XLOG_MULTIXACT_CREATE_ID`, `XLOG_MULTIXACT_TRUNCATE_ID`), the relmap record (`XLOG_RELMAP_UPDATE`), the storage records (`XLOG_SMGR_CREATE`, `XLOG_SMGR_TRUNCATE`), the heap-visible record (`XLOG_HEAP2_VISIBLE`), the commit-ts records (`COMMIT_TS_ZEROPAGE`, `COMMIT_TS_TRUNCATE`, `COMMIT_TS_SETTS`), and the dbase / tablespace records (`XLOG_DBASE_*`, `XLOG_TBLSPC_*`). For each: rmgr ID, info-byte, payload struct, redo function, and the metadata it makes durable.

## Output Directory
All generated artifacts — intermediate files (architecture_map.json, key_symbols.txt, etc.), component files, diagrams, and final documentation modules — **must** be written under the following directory:

```
topic_specific_generated_docs/about_metadata/
```

Create this directory at the start of Stage 1 if it does not already exist. Use subdirectories as needed:

```
topic_specific_generated_docs/about_metadata/
├── stage1/                              # Architecture analysis outputs
│   ├── architecture_map.json
│   ├── key_symbols.txt
│   ├── initial_outline.md
│   ├── catalog_inventory.txt            # Every pg_catalog table with header + .dat + indexes
│   ├── slru_inventory.txt               # Every SLRU-backed file with disk path + page formula
│   └── wal_record_inventory.txt         # Every metadata-affecting WAL record with rmgr + info byte
├── stage2/                              # Detailed documentation components
│   ├── component_*.md
│   ├── catalog_inventory/                # Per-pg_catalog-table documentation
│   │   ├── core_relations.md            # pg_class, pg_attribute, pg_index, pg_namespace, pg_database, ...
│   │   ├── type_system.md               # pg_type, pg_cast, pg_range, pg_enum, pg_collation, pg_conversion
│   │   ├── functions_and_operators.md   # pg_proc, pg_aggregate, pg_operator, pg_amop, pg_amproc, pg_opclass, pg_opfamily, pg_am, pg_language
│   │   ├── constraints_and_dependencies.md  # pg_constraint, pg_depend, pg_shdepend, pg_attrdef, pg_inherits
│   │   ├── partitioning.md              # pg_partitioned_table, pg_inherits, pg_class.relpartbound
│   │   ├── statistics.md                # pg_statistic, pg_statistic_ext, pg_statistic_ext_data
│   │   ├── access_control.md            # pg_authid, pg_auth_members, pg_database, pg_tablespace, pg_default_acl, pg_init_privs, pg_policy, pg_seclabel, pg_shseclabel, pg_parameter_acl
│   │   ├── replication_and_publication.md  # pg_publication, pg_publication_*, pg_subscription, pg_subscription_rel, pg_replication_origin
│   │   ├── triggers_and_rewrite.md      # pg_trigger, pg_event_trigger, pg_rewrite
│   │   ├── extensions_and_fdw.md        # pg_extension, pg_foreign_data_wrapper, pg_foreign_server, pg_foreign_table, pg_user_mapping, pg_transform
│   │   ├── text_search.md               # pg_ts_config, pg_ts_config_map, pg_ts_dict, pg_ts_parser, pg_ts_template
│   │   └── misc.md                      # pg_largeobject, pg_largeobject_metadata, pg_db_role_setting, pg_description, pg_shdescription
│   ├── slru_users_catalog/              # Per-SLRU-instance documentation
│   │   ├── clog.md                       # commit/abort status (pg_xact)
│   │   ├── subtrans.md                   # subtransaction parent (pg_subtrans)
│   │   ├── multixact_offsets.md          # MultiXactId → first-member offset (pg_multixact/offsets)
│   │   ├── multixact_members.md          # multixact members array (pg_multixact/members)
│   │   ├── commit_ts.md                  # commit timestamp + replication origin (pg_commit_ts)
│   │   └── other_slru_users.md           # async (pg_notify), predicate (pg_serial)
│   ├── wal_record_catalog/              # Per-WAL-record documentation
│   │   ├── clog_records.md
│   │   ├── multixact_records.md
│   │   ├── relmap_records.md
│   │   ├── storage_smgr_records.md      # XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE
│   │   ├── heap_visible_records.md      # XLOG_HEAP2_VISIBLE
│   │   ├── commit_ts_records.md
│   │   ├── nextoid_and_checkpoint_records.md
│   │   └── dbase_and_tblspc_records.md
│   └── diagrams/
│       └── *.mermaid
├── final/                               # Integrated final documentation
│   ├── index.md
│   ├── 01_executive_summary.md
│   ├── ...
│   ├── 20_deep_dives.md
│   ├── appendix_*.md
│   ├── metadata_quick_reference.md
│   ├── metadata_api_reference.md
│   └── quality_report.md
└── diagrams/                            # Final consolidated diagrams
    └── *.mermaid
```

**All file paths referenced between stages (e.g., Stage 2 reading Stage 1 outputs) must use paths relative to `topic_specific_generated_docs/about_metadata/`.**

## Available Resources

### Local Source Code (PostgreSQL `src/` directory)
The PostgreSQL source tree is available locally at `./src/`. This is a direct copy of the upstream `src/` directory and should be actively referenced throughout all stages. Key directories for Metadata documentation:

| Directory | Contents |
|---|---|
| `src/backend/catalog/` | **System catalog implementation** — `heap.c` (heap_create_with_catalog, heap_create, heap_drop_with_catalog, AddNewRelationTuple, AddNewAttributeTuples, RelationRemoveInheritance, …), `index.c` (index_create, index_drop, index_constraint_create, index_update_stats, IndexSetParentIndex, …), `indexing.c` (CatalogIndexInsert, CatalogTupleInsert, CatalogTupleUpdate, CatalogTupleDelete — the canonical write-with-index API), `dependency.c` (recordDependencyOn, recordSharedDependencyOn, deleteDependencyRecordsFor, performDeletion, AcquireDeletionLock, findDependentObjects), `namespace.c` (RangeVarGetRelid, LookupExplicitNamespace, NamespaceLookupNamespace, get_namespace_oid, recomputeNamespacePath), `partition.c` (RelationGetPartitionDesc, partition_bounds_create), `storage.c` (RelationCreateStorage, RelationDropStorage, log_smgrcreate, smgrDoPendingDeletes — ties catalog + storage manager), `toasting.c` (NewHeapCreateToastTable, create_toast_table), `aclchk.c` (ExecuteGrantStmt, pg_class_aclmask, pg_namespace_aclcheck, …), `objectaddress.c` (get_object_address, AlterObjectNamespace_internal), `objectaccess.c` (RunObjectPostCreateHook, RunObjectDropHook), `pg_aggregate.c`/`pg_attrdef.c`/`pg_cast.c`/`pg_class.c`/`pg_collation.c`/`pg_constraint.c`/`pg_conversion.c`/`pg_db_role_setting.c`/`pg_depend.c`/`pg_enum.c`/`pg_inherits.c`/`pg_largeobject.c`/`pg_namespace.c`/`pg_operator.c`/`pg_parameter_acl.c`/`pg_proc.c`/`pg_publication.c`/`pg_range.c`/`pg_shdepend.c`/`pg_subscription.c`/`pg_type.c` (per-catalog helpers — the focused mutation routines that the higher-level callers use) |
| `src/backend/utils/cache/` | **Catalog caches and invalidation** — `catcache.c` (the bottom-level negative-cache-aware syscache: `CatCache`, `CatCList`, `SearchCatCache`, `SearchCatCacheList`, `CatalogCacheInitializeCache`, `CatCacheInvalidate`, `CatCacheRemoveCList`, `CatCacheRemoveCTup`), `syscache.c` (the typed wrapper: `cacheinfo[]` table indexed by `SysCacheIdentifier`, `SearchSysCache1..4`, `SearchSysCacheCopy*`, `SearchSysCacheLocked*`, `SearchSysCacheAttName*`, `SearchSysCacheAttNum*`, `SysCacheGetAttr`, `SysCacheGetAttrNotNull`), `relcache.c` (the heavy `Relation` cache: `RelationIdGetRelation`, `RelationClose`, `RelationBuildDesc`, `RelationCacheInitialize*`, `RelationClearRelation`, `RelationFlushRelation`, `RelationCacheInvalidate`, `formrdesc` for nailed entries, `load_critical_index`, `RelationCacheInitFilePostInvalidate`, `RelationCacheInitFileInvalidate`, the `pg_internal.init` initial-cache file), `inval.c` (the per-transaction invalidation queue: `CacheInvalidateRelcache`, `CacheInvalidateHeapTuple`, `CacheInvalidateCatalog`, `RegisterCatcacheInvalidation`, `RegisterRelcacheInvalidation`, `PrepareInvalidationState`, `xactGetCommittedInvalidationMessages`, `ProcessCommittedInvalidationMessages`, `AtEOXact_Inval`, `AtEOSubXact_Inval`, `LocalExecuteInvalidationMessage`, `InvalidateSystemCaches`), `plancache.c` (cached plans: `CreateCachedPlan`, `RevalidateCachedQuery`, `GetCachedPlan`), `partcache.c` (partition descriptor cache), `typcache.c` (composite-type and domain cache), `evtcache.c` (event-trigger cache), `attoptcache.c`, `spccache.c`, `ts_cache.c`, `relfilenumbermap.c` (RelFileNumber → Oid reverse lookup), `relmapper.c` (the only metadata that is **not** stored in pg_class.relfilenode but in `pg_filenode.map` files: `RelationMapOidToFilenumber`, `RelationMapUpdateMap`, `CommitTransactionRelationMap`, `RelationMapFinishBootstrap`, `write_relmap_file`, `load_relmap_file`, `relmap_redo`) |
| `src/backend/storage/ipc/` | **Shared invalidation distribution** — `sinval.c` (`SendSharedInvalidMessages`, `ReceiveSharedInvalidMessages`, `SICatchupCallback`), `sinvaladt.c` (the shared-memory ring buffer + per-backend cursor: `SInvalShmemSize`, `CreateSharedInvalidationState`, `SIInsertDataEntries`, `SIGetDataEntries`, `SIInsertDataEntry`, sinval queue overflow → `RESET`) |
| `src/backend/access/transam/` | **Commit log family + SLRU + bootstrap + WAL emission for metadata** — `slru.c` (the shared infrastructure: `SimpleLruInit`, `SimpleLruZeroPage`, `SimpleLruReadPage`, `SimpleLruReadPage_ReadOnly`, `SimpleLruWritePage`, `SimpleLruWriteAll`, `SimpleLruTruncate`, `SimpleLruDoesPhysicalPageExist`, `SlruScanDirectory`, `SlruInternalWritePage`, `SimpleLruWaitIO`, `SlruPhysicalReadPage`, `SlruPhysicalWritePage`, `SlruSyncFileTag`, `SlruSelectLRUPage`), `clog.c` (`TransactionIdSetTreeStatus`, `TransactionIdSetPageStatus`, `TransactionIdSetPageStatusInternal`, `TransactionIdSetStatusBit`, `TransactionIdGetStatus`, `TransactionGroupUpdateXidStatus`, `CLOGShmemInit`, `BootStrapCLOG`, `StartupCLOG`, `TrimCLOG`, `CheckPointCLOG`, `ExtendCLOG`, `TruncateCLOG`, `clog_redo`, `WriteZeroPageXlogRec`, `WriteTruncateXlogRec`), `subtrans.c` (`SubTransSetParent`, `SubTransGetParent`, `SubTransGetTopmostTransaction`, `SUBTRANSShmemInit`, `BootStrapSUBTRANS`, `StartupSUBTRANS`, `CheckPointSUBTRANS`, `ExtendSUBTRANS`, `TruncateSUBTRANS`), `commit_ts.c` (`TransactionTreeSetCommitTsData`, `TransactionIdSetCommitTs`, `TransactionIdGetCommitTsData`, `GetLatestCommitTsData`, `CommitTsShmemInit`, `BootStrapCommitTs`, `StartupCommitTs`, `CheckPointCommitTs`, `ExtendCommitTs`, `TruncateCommitTs`, `commit_ts_redo`, `WriteZeroPageXlogRec`, `WriteTruncateXlogRec`), `multixact.c` (`MultiXactIdCreate`, `MultiXactIdExpand`, `MultiXactIdIsRunning`, `GetNewMultiXactId`, `RecordNewMultiXact`, `MultiXactIdGetUpdateXid`, `MultiXactGetMembers`, `mXactCacheGetById`, `mXactCacheGetBySet`, `MultiXactShmemInit`, `BootStrapMultiXact`, `StartupMultiXact`, `TrimMultiXact`, `CheckPointMultiXact`, `ExtendMultiXactOffset`, `ExtendMultiXactMember`, `TruncateMultiXact`, `multixact_redo`, `MultiXactSetNextMXact`, `MultiXactAdvanceNextMXact`, `MultiXactAdvanceOldest`, `SetOffsetVacuumLimit`, `MultiXactMemberFreezeThreshold`), `varsup.c` (`GetNewTransactionId`, `ReadNextTransactionId`, `SetTransactionIdLimit`, `AssertTransactionIdInAllowableRange`, `ForceTransactionIdLimitUpdate`, `GetNewObjectId`, `GetNewMultiXactId`, `WrapLimitsVacuum` interplay), `transam.c` (`TransactionIdDidCommit`, `TransactionIdDidAbort`, `TransactionIdIsKnownCompleted`, `TransactionIdAbort`, `TransactionIdCommitTree`, `TransactionIdAsyncCommitTree`, `TransactionIdSetTreeStatus` wrapper), `xact.c` (`RecordTransactionCommit`, `RecordTransactionAbort`, `RecordTransactionAbortPrepared` — the actual WAL emission for transaction completion that drives clog updates), `xlog.c` and `xloginsert.c` (the WAL machinery underneath), `xlogrecovery.c` (`StartupXLOG`, `PerformWalRecovery`, the redo loop), `rmgr.c` and `include/access/rmgrlist.h` (resource manager dispatch table) |
| `src/backend/access/heap/` | **Visibility map + heap visibility checks + heap-buffer FSM lookup** — `visibilitymap.c` (the bit-vector pages: `visibilitymap_clear`, `visibilitymap_pin`, `visibilitymap_pin_ok`, `visibilitymap_set`, `visibilitymap_get_status`, `visibilitymap_count`, `visibilitymap_prepare_truncate`, `vm_readbuf`, `vm_extend`), `heapam_visibility.c` (`HeapTupleSatisfiesMVCC`, `HeapTupleSatisfiesSelf`, `HeapTupleSatisfiesAny`, `HeapTupleSatisfiesToast`, `HeapTupleSatisfiesUpdate`, `HeapTupleSatisfiesDirty`, `HeapTupleSatisfiesNonVacuumable`, `HeapTupleSatisfiesHistoricMVCC`, `HeapTupleSatisfiesVacuum*`, `HeapTupleHeaderAdvanceConflictHorizon` — these consume CLOG/MultiXact metadata to decide visibility), `heapam.c` (the mutation paths that *clear* VM bits via `visibilitymap_clear`: `heap_insert`, `heap_multi_insert`, `heap_update`, `heap_delete`, `heap_lock_tuple`), `pruneheap.c` (`heap_page_prune_opt`, `heap_page_prune` — the HOT-prune path that may *not* clear all-visible if no actual visibility change), `vacuumlazy.c` (the path that *sets* VM bits: `lazy_scan_heap`, `lazy_scan_prune`, `lazy_vacuum_heap_page`, `heap_vac_scan_next_block`, `vacuum_log_heap_freeze`, the `XLOG_HEAP2_VISIBLE` emitter), `hio.c` (`RelationGetBufferForTuple`, `GetVisibilityMapPins` — the heap-extension fast path that consults FSM and pins VM pages before locking) |
| `src/backend/storage/freespace/` | **Free space map** — `freespace.c` (the public API and tree-walk: `GetPageWithFreeSpace`, `GetRecordedFreeSpace`, `RecordPageWithFreeSpace`, `RecordAndGetPageWithFreeSpace`, `XLogRecordPageWithFreeSpace`, `FreeSpaceMapPrepareTruncateRel`, `FreeSpaceMapVacuum`, `FreeSpaceMapVacuumRange`, `fsm_get_location`, `fsm_logical_to_physical`, `fsm_get_parent`, `fsm_get_child`, `fsm_search`, `fsm_set_and_search`, `fsm_vacuum_page`, `fsm_readbuf`, `fsm_extend`, `fsm_space_avail_to_cat`, `fsm_space_cat_to_avail`, `fsm_space_needed_to_cat`), `fsmpage.c` (the page-internal binary heap: `fsm_search_avail`, `fsm_set_avail`, `fsm_truncate_avail`, `fsm_get_avail`, `fsm_rebuild_page`), `indexfsm.c` (`GetFreeIndexPage`, `RecordFreeIndexPage`, `RecordUsedIndexPage`, `IndexFreeSpaceMapVacuum`) |
| `src/backend/utils/init/` | **Cluster bootstrap, control file consultation** — `globals.c` (controlling Oids and TransactionIds), `miscinit.c` (`SetDataDir`, lock/PID file management), `postinit.c` (`InitPostgres`, `InitializeMaxBackends`, `BaseInit`, `InitTempTableNamespace`) |
| `src/backend/bootstrap/` | **`postgres --boot` and the BKI loader** — `bootstrap.c`, `bootparse.y`, `bootscanner.l` (consume `postgres.bki`, create initial pg_class, pg_attribute, pg_proc, pg_type rows that genbki.pl emitted from `.dat` files) |
| `src/include/catalog/` | **Catalog headers + `.dat` initialization data + `pg_control` schema** — every `pg_*.h` declares the C-level rowtype (e.g., `FormData_pg_class`, `Form_pg_class`, `Anum_pg_class_*`, `Natts_pg_class`); every `pg_*.dat` provides bootstrap rows; `indexing.h` lists every system index (`DECLARE_*INDEX*` macros and the corresponding Oids); `genbki.h` defines the macros consumed by `genbki.pl`; `pg_control.h` defines `ControlFileData` (the cluster-wide control file `global/pg_control` that anchors recovery: `system_identifier`, `state`, `checkPoint`, `redo`, `nextXid`, `oldestXid`, `nextMulti`, `nextMultiOffset`, `oldestMulti`, `oldestCommitTsXid`, `newestCommitTsXid`, `wal_level`, etc.); `catversion.h` defines `CATALOG_VERSION_NO`; `storage.h`/`storage_xlog.h` carries `XLOG_SMGR_CREATE` / `XLOG_SMGR_TRUNCATE` / `xl_smgr_create` / `xl_smgr_truncate`; `dependency.h` defines `ObjectAddress`, `DependencyType`, `SharedDependencyType`; `objectaddress.h`; `partition.h`; `namespace.h` |
| `src/include/access/` | **Metadata-relevant headers** — `clog.h` (`CLOG_XACTS_PER_PAGE`, `XidStatus`, `xl_clog_truncate`, redo prototype), `slru.h` (`SlruCtlData`, `SlruSharedData`, `SimpleLruGetBankLock`, `SLRU_PAGES_PER_SEGMENT`, `SLRU_BANK_BITSHIFT`, `SLRU_NUM_BANKS`), `subtrans.h`, `commit_ts.h` (`COMMIT_TS_XACTS_PER_PAGE`, `xl_commit_ts_set`, `xl_commit_ts_truncate`), `multixact.h` (`MULTIXACT_OFFSETS_PER_PAGE`, `MULTIXACT_MEMBERS_PER_PAGE`, `MultiXactStatus`, `MultiXactMember`, `xl_multixact_create`, `xl_multixact_truncate`), `transam.h` (`InvalidTransactionId`, `BootstrapTransactionId`, `FrozenTransactionId`, `FirstNormalTransactionId`, `TransactionIdPrecedes`, `TransactionIdFollows`), `visibilitymap.h` and `visibilitymapdefs.h` (`VISIBILITYMAP_ALL_VISIBLE`, `VISIBILITYMAP_ALL_FROZEN`, `HEAPBLOCKS_PER_BYTE`, `HEAPBLOCKS_PER_PAGE`), `xact.h` (`XactCallbackEvent`, `RegisterXactCallback`, `XLOG_XACT_COMMIT`, `XLOG_XACT_ABORT`, `XLOG_XACT_PREPARE`, `xl_xact_*` payload structs), `xlog.h`, `xlog_internal.h`, `xlogrecord.h`, `xlogreader.h`, `xlogrecovery.h`, `rmgrlist.h` (the master rmgr table — read this end-to-end to discover every redo handler the system has), `heapam_xlog.h` (`XLOG_HEAP2_VISIBLE`, `xl_heap_visible`) |
| `src/include/storage/` | **FSM/VM headers and storage manager** — `freespace.h` (the public FSM API), `bufmgr.h`/`bufpage.h`, `smgr.h` (the storage-manager abstraction underneath all on-disk metadata), `md.h` (the magnetic-disk smgr implementation), `relfilelocator.h` (`RelFileLocator`, `RelFileNumber`), `sinval.h`, `sinvaladt.h`, `lwlock.h` (the lock partitioning that protects SLRU banks, sinval queue, etc.) |
| `src/include/utils/` | **Cache headers** — `catcache.h`, `syscache.h` (the `SysCacheIdentifier` enum — every keyed catalog access goes through one of these), `relcache.h`, `inval.h`, `plancache.h`, `partcache.h`, `typcache.h`, `evtcache.h`, `relmapper.h` (`XLOG_RELMAP_UPDATE`, `xl_relmap_update`, `RELMAPPER_FILEMAGIC`), `rel.h` (`RelationData`, `Relation`, the in-memory representation of a relcache entry and the gateway to `RelationData->rd_rel` (Form_pg_class), `rd_att` (TupleDesc), `rd_indexlist`, `rd_pkattr`, `rd_partdesc`, `rd_pubdesc`, etc.) |
| `src/backend/access/transam/README` | **The single most important reading material for the SLRU/CLOG/MultiXact/CommitTs subsystems and for transaction-level WAL emission semantics. ~900 lines.** |
| `src/backend/storage/freespace/README` | **Authoritative description of the FSM tree, the heap-extension fast path, and FSM concurrency. ~200 lines. Read in full.** |
| `src/backend/access/heap/README.HOT` | **HOT prune mechanics — how the VM's all-visible bit interacts with HOT chains; how prune does or does not invalidate it.** |
| `src/backend/utils/cache/syscache.c` (top comment) | **Authoritative description of how syscache is layered on catcache, how each entry is keyed, and the invalidation contract.** |
| `src/include/catalog/pg_control.h` (top comment) | **Authoritative description of the cluster-wide control file — what is in it, when it is updated, and why it is the recovery anchor.** |

**Usage guidelines for source code**:
- When documenting a function, always verify its actual signature and logic against the local source (`./src/...`) as the ground truth.
- Use `grep -rn` to discover call sites, `#define` constants, and struct definitions.
- When quoting source code in documentation, include the relative file path (e.g., `src/backend/access/transam/clog.c:735`) for traceability.
- **For the catalog inventory**: enumerate by listing `src/include/catalog/pg_*.h` (header per relation), cross-checking with `src/include/catalog/pg_*.dat` (boot data), with `src/include/catalog/indexing.h` (`DECLARE_*INDEX*`) for index Oids, and with the corresponding `src/backend/catalog/pg_*.c` for the C-side update helper. Note which catalogs are shared (`#define CATALOG_NAME_SHARED` in the BKI hint, e.g., pg_database, pg_authid, pg_tablespace) and which are mapped (`MakeRelMapper` calls in bootstrap — pg_class, pg_attribute, pg_proc, pg_type for shared relations, and the same four catalogs of every database).
- **For the SLRU inventory**: enumerate by `grep -nE 'SimpleLruInit|SlruCtlData' src/backend/access/transam/*.c src/backend/commands/async.c src/backend/storage/lmgr/predicate.c`. Each call to `SimpleLruInit` corresponds to one SLRU instance. Cross-check against `src/include/access/slru.h` constants and the on-disk directory names declared in each `*ShmemInit`.
- **For the WAL record inventory**: enumerate by reading `src/include/access/rmgrlist.h` end-to-end, then for every rmgr touching metadata (`RM_XLOG_ID`, `RM_XACT_ID`, `RM_SMGR_ID`, `RM_CLOG_ID`, `RM_DBASE_ID`, `RM_TBLSPC_ID`, `RM_MULTIXACT_ID`, `RM_RELMAP_ID`, `RM_HEAP2_ID`, `RM_COMMIT_TS_ID`) extract the info-byte constants from the corresponding header.

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL Metadata subsystem architecture (system catalogs, CLOG and
its SLRU siblings, Visibility Map, Free Space Map, and the persistence guarantees
that make all of them crash-safe).

Use the local source tree (`./src/`) for analysis. Do not depend on MCP tools — if
they fail, fall back to direct source reading and `grep`.

**Source exploration strategy for this stage**:
- Begin by reading three foundational documents end-to-end:
  - `src/backend/access/transam/README` (~900 lines) — covers WAL, transaction
    commit, the SLRU-backed CLOG/SUBTRANS/MultiXact/CommitTs subsystems, and the
    crash-recovery contract. **This is the single most important reading.**
  - `src/backend/storage/freespace/README` (~200 lines) — the FSM tree.
  - `src/backend/access/heap/README.HOT` — for VM/HOT interaction.
- Read the top file-comment of `src/backend/utils/cache/syscache.c` and
  `src/include/catalog/pg_control.h` for the catalog-cache contract and the
  cluster-control-file schema.
- Scan key directories to identify relevant files:
  - `find ./src/backend/catalog/ -name '*.c'`
  - `find ./src/backend/utils/cache/ -name '*.c'`
  - `find ./src/backend/storage/ipc/ -name 'sinval*.c'`
  - `find ./src/backend/access/transam/ -name '*.c'`
  - `find ./src/backend/access/heap/ -name '*.c'`
  - `find ./src/backend/storage/freespace/ -name '*.c'`
  - `find ./src/include/catalog/ -name 'pg_*.h' | sort`
  - `find ./src/include/catalog/ -name 'pg_*.dat' | sort`
  - `find ./src/include/access/ -name '*.h'`
- Read these key headers end-to-end:
  - `src/include/utils/rel.h` (RelationData — what a relcache entry looks like)
  - `src/include/utils/syscache.h` (SysCacheIdentifier enum)
  - `src/include/utils/catcache.h` (CatCache, CatCList)
  - `src/include/utils/inval.h` (SharedInvalidationMessage, callback registration)
  - `src/include/utils/relmapper.h` (xl_relmap_update, RELMAPPER_FILEMAGIC)
  - `src/include/access/slru.h` (SlruCtlData, SlruSharedData, page-bank locking)
  - `src/include/access/clog.h`, `subtrans.h`, `commit_ts.h`, `multixact.h`,
    `transam.h`, `xact.h`, `visibilitymap.h`, `visibilitymapdefs.h`, `rmgrlist.h`
  - `src/include/storage/freespace.h`, `smgr.h`, `relfilelocator.h`, `sinval.h`
  - `src/include/catalog/indexing.h` (master list of every system index),
    `dependency.h`, `objectaddress.h`, `pg_control.h`, `catversion.h`,
    `storage_xlog.h`
- Use `grep -rn 'FunctionName' ./src/` to trace call chains and discover symbols.
- Enumerate every system catalog:
  `ls src/include/catalog/pg_*.h | sed 's@.*/@@' | sort`
  Cross-check with `ls src/include/catalog/pg_*.dat` and look for matching
  `src/backend/catalog/pg_*.c` C-side helpers. Note shared / nailed / mapped status.
- Enumerate every SLRU instance:
  `grep -nE 'SimpleLruInit\(' src/backend/access/transam/*.c src/backend/commands/async.c src/backend/storage/lmgr/predicate.c`
  Each match is one SLRU instance; record the SlruCtl pointer name, the on-disk
  directory (the second argument to SimpleLruInit), and the page-number formula
  (the *ToPage helper near the top of the file).
- Enumerate every metadata-affecting WAL record by reading
  `src/include/access/rmgrlist.h` (the master PG_RMGR list) and then, for each
  metadata-related rmgr, extracting the info-byte #defines from its header
  (`XLOG_CLOG_*`, `XLOG_MULTIXACT_*`, `XLOG_RELMAP_UPDATE`, `XLOG_SMGR_*`,
  `XLOG_HEAP2_VISIBLE`, `COMMIT_TS_*`, `XLOG_DBASE_*`, `XLOG_TBLSPC_*`,
  `XLOG_NEXTOID`, `XLOG_CHECKPOINT_*`, `XLOG_FPI`, `XLOG_FPI_FOR_HINT`).

Build a comprehensive dependency map with depth 5 traversal. Focus on:

1. System-catalog data model and bootstrap
   - `genbki.pl` reading every `pg_*.h` (BKI macro hints) and every `pg_*.dat`
     (boot rows), emitting `postgres.bki` consumed by `postgres --boot`
   - `bootstrap.c` / `bootparse.y` / `bootscanner.l` consuming `postgres.bki`
   - The "nailed" relations — `pg_class`, `pg_attribute`, `pg_proc`, `pg_type`,
     plus the shared variants pg_database / pg_authid / pg_tablespace —
     installed via `formrdesc()` in `relcache.c` so the relcache can bootstrap
     without itself needing a usable relcache
   - `catversion.h` (`CATALOG_VERSION_NO`) and the on-disk compatibility check
   - `pg_control.h` (`ControlFileData`) — the cluster anchor: system_identifier,
     state, checkPoint, redo, nextXid, oldestXid, nextMulti, nextMultiOffset,
     oldestMulti, oldestCommitTsXid, newestCommitTsXid, wal_level

2. Catalog modification APIs
   - `catalog/heap.c` — heap_create_with_catalog, heap_create, heap_drop_with_catalog,
     AddNewRelationTuple, AddNewAttributeTuples, RelationRemoveInheritance,
     CheckAttributeType
   - `catalog/index.c` — index_create, index_drop, index_constraint_create,
     index_update_stats, IndexSetParentIndex, ReindexRelationConcurrently
   - `catalog/indexing.c` — CatalogIndexInsert, CatalogTupleInsert,
     CatalogTupleInsertWithInfo, CatalogTupleUpdate, CatalogTupleUpdateWithInfo,
     CatalogTupleDelete (these are the only sanctioned catalog mutators —
     they keep heap and indexes in sync)
   - `catalog/dependency.c` — recordDependencyOn, recordSharedDependencyOn,
     deleteDependencyRecordsFor, performDeletion, performMultipleDeletions,
     findDependentObjects, AcquireDeletionLock, ReleaseDeletionLock,
     getOwnedSequences, getDependentObjects
   - `catalog/namespace.c` — RangeVarGetRelid, RangeVarGetCreationNamespace,
     LookupExplicitNamespace, get_namespace_oid, recomputeNamespacePath,
     ResolveOpClass, AccessTempTableNamespace
   - `catalog/storage.c` — RelationCreateStorage, RelationDropStorage,
     log_smgrcreate, smgrDoPendingDeletes, smgrDoPendingSyncs
     (the bridge from catalog row creation to physical relfilenode existence)
   - `catalog/toasting.c` — NewHeapCreateToastTable, create_toast_table
   - `catalog/aclchk.c` — ExecGrantStmt_oids, pg_class_aclmask,
     pg_namespace_aclmask, ACL parsing/encoding
   - `catalog/objectaddress.c` — get_object_address, get_object_address_*
   - `catalog/objectaccess.c` — RunObjectPostCreateHook, RunObjectDropHook
   - `catalog/partition.c` — RelationGetPartitionDesc, partition_bounds_create
   - The per-catalog helpers `pg_aggregate.c`, `pg_attrdef.c`, `pg_cast.c`,
     `pg_class.c`, `pg_collation.c`, `pg_constraint.c`, `pg_conversion.c`,
     `pg_db_role_setting.c`, `pg_depend.c`, `pg_enum.c`, `pg_inherits.c`,
     `pg_largeobject.c`, `pg_namespace.c`, `pg_operator.c`, `pg_parameter_acl.c`,
     `pg_proc.c`, `pg_publication.c`, `pg_range.c`, `pg_shdepend.c`,
     `pg_subscription.c`, `pg_type.c`

3. Catalog-cache stack
   - catcache.c — CatCache, CatCList, hash table per cache,
     CatalogCacheInitializeCache, SearchCatCacheInternal,
     SearchCatCacheList, CatCacheRemoveCList, CatCacheRemoveCTup,
     ResetCatalogCaches, CatCacheInvalidate, negative entries
   - syscache.c — the cacheinfo[] table, SysCacheIdentifier enum, the
     SearchSysCache1..4 fast-path wrappers, SearchSysCacheCopy*,
     SearchSysCacheLocked* (the locked variant takes a row-level lock to
     prevent concurrent overwrite), SearchSysCacheAttName / AttNum,
     SysCacheGetAttr, SysCacheGetAttrNotNull, GetSysCacheOid,
     GetSysCacheHashValue, RelationInvalidatesSnapshotsOnly,
     RelationHasSysCache, RelationSupportsSysCache
   - relcache.c — RelationData, RelationIdGetRelation, RelationClose,
     RelationBuildDesc, RelationInitTableAccessMethod,
     RelationCacheInitialize / InitializePhase2 / InitializePhase3,
     RelationClearRelation, RelationFlushRelation,
     RelationCacheInvalidate, formrdesc (nailed catalog bootstrap),
     load_critical_index, write_relcache_init_file,
     RelationCacheInitFilePostInvalidate, RelationCacheInitFileRemove,
     `pg_internal.init` (the per-database relcache init file that lets
     subsequent backends skip catalog-driven relcache rebuild)
   - plancache.c — CreateCachedPlan, RevalidateCachedQuery, GetCachedPlan,
     ReleaseCachedPlan, CachedPlanIsSimplyValid
   - partcache.c — RelationGetPartitionDesc
   - typcache.c — lookup_type_cache, COMPOSITE row-type cache,
     domain-constraint cache, range-type cache
   - evtcache.c, attoptcache.c, spccache.c, ts_cache.c
   - relfilenumbermap.c — RelidByRelfilenumber (reverse lookup
     RelFileNumber → Oid for WAL replay / debugging tools)

4. Cache invalidation (the contract that keeps caches consistent across
   transactions, backends, and replicas)
   - inval.c — the per-transaction outbox:
       a. CacheInvalidateRelcache, CacheInvalidateHeapTuple,
          CacheInvalidateHeapTupleByRelid, CacheInvalidateCatalog
       b. RegisterCatcacheInvalidation, RegisterRelcacheInvalidation,
          RegisterSnapshotInvalidation
       c. xactGetCommittedInvalidationMessages — emit messages with the commit
          WAL record so the standby and other backends can apply them
       d. ProcessCommittedInvalidationMessages — replay path
       e. AtEOXact_Inval (commit / abort), AtEOSubXact_Inval
       f. PrepareInvalidationState, AddRelcacheInvalidationMessage,
          AddCatcacheInvalidationMessage, AddSnapshotInvalidationMessage
       g. LocalExecuteInvalidationMessage, InvalidateSystemCaches,
          InvalidateSystemCachesExtended
   - sinval.c / sinvaladt.c — the shared-memory ring buffer that distributes
     invalidations across backends:
       - SendSharedInvalidMessages, ReceiveSharedInvalidMessages
       - SInvalShmemSize, CreateSharedInvalidationState, SharedInvalidStateData
       - SIInsertDataEntries, SIGetDataEntries, the per-backend cursor
       - sinval queue overflow → SI_RESET (force every backend to invalidate
         all caches)
   - The commit-time emission of invalidation messages alongside
     `XLOG_XACT_COMMIT` (in `xact.c`'s `RecordTransactionCommit`) — this is
     how standbys learn about catalog changes

5. Relmapper — the metadata-of-the-metadata
   - `pg_filenode.map` files (one in each database directory plus one in
     `global/`) carry the RelFileNumber for relations that **cannot** be
     tracked through `pg_class.relfilenode` — namely the four nailed catalogs
     pg_class / pg_attribute / pg_proc / pg_type and the three shared catalogs
     pg_database / pg_authid / pg_tablespace
   - relmapper.c — RelMapping, RelMapFile (with magic + CRC),
     load_relmap_file, write_relmap_file (atomic via temp+rename+fsync),
     RelationMapOidToFilenumber, RelationMapUpdateMap,
     CommitTransactionRelationMap, AtPrepare_RelationMap,
     RelationMapInitialize / InitializePhase2 / InitializePhase3
   - relmap_redo — replays XLOG_RELMAP_UPDATE on standbys
   - The atomicity guarantee: write to .tmp, fsync, rename, fsync directory

6. SLRU framework (the shared infrastructure for all xact-key-indexed metadata)
   - `SlruCtlData` / `SlruSharedData` — fixed slot count, page-bank locking
     (one LWLock per bank of pages, controlled by `SLRU_BANK_BITSHIFT`),
     hash-based slot lookup, page-status state machine
     (EMPTY → READ_IN_PROGRESS → VALID → WRITE_IN_PROGRESS → DIRTY)
   - SimpleLruInit — set up name, slot count, lsn-tracking slots,
     on-disk directory, sync-handler registration
   - SimpleLruZeroPage — initialize a fresh page (used during Extend* and
     during redo of XLOG_*_ZEROPAGE records)
   - SimpleLruReadPage / SimpleLruReadPage_ReadOnly — fetch a page,
     possibly reading from disk
   - SimpleLruWritePage / SimpleLruInternalWritePage / SlruPhysicalWritePage
     — flush a dirty slot to disk; used by checkpoint and by slot reuse
   - SimpleLruWriteAll — checkpoint hook
   - SimpleLruTruncate — remove segment files older than a cutoff page
   - SlruScanDirectory — iterate segment files (used during Truncate and
     during diagnostic scans)
   - The on-disk segment layout: `SLRU_PAGES_PER_SEGMENT` pages per segment file,
     filename is the segment number in hex
   - Bank locking: each bank has its own LWLock; SimpleLruGetBankLock partitions
     pages by their low bits

7. CLOG (clog.c) — commit/abort/sub-committed status per XID
   - 2 bits per XID (`CLOG_XACTS_PER_PAGE = BLCKSZ * 4`)
   - TransactionIdSetTreeStatus → TransactionIdSetPageStatus →
     TransactionIdSetPageStatusInternal → TransactionIdSetStatusBit
   - TransactionIdGetStatus — read path used by visibility checks and
     SubTransGetTopmostTransaction
   - TransactionGroupUpdateXidStatus — group-commit batching for
     concurrent committers updating the same page
   - ExtendCLOG — called from GetNewTransactionId to zero a fresh CLOG page
     and emit XLOG_CLOG_ZEROPAGE
   - TruncateCLOG — called from vac_truncate_clog after vacuum advances
     the cluster-wide oldestXid; emits XLOG_CLOG_TRUNCATE
   - CheckPointCLOG — flush all dirty CLOG pages to disk during checkpoint
   - clog_redo — replay XLOG_CLOG_ZEROPAGE / XLOG_CLOG_TRUNCATE
   - CLOGShmemInit / BootStrapCLOG / StartupCLOG / TrimCLOG —
     bootstrap and recovery path

8. SUBTRANS (subtrans.c) — subtransaction parent linkage
   - 4 bytes per XID (one TransactionId per subtransaction)
   - SubTransSetParent, SubTransGetParent, SubTransGetTopmostTransaction
   - **Not WAL-logged** — subtrans is reconstructed during recovery via
     normal SubTransSetParent calls; the on-disk file is treated as
     truncated-and-rebuilt
   - ExtendSUBTRANS, TruncateSUBTRANS — called as part of XID lifecycle
   - StartupSUBTRANS — wipes pages older than oldestActiveXid

9. Commit Timestamps (commit_ts.c)
   - 10 bytes per XID (TimestampTz + RepOriginId)
   - TransactionTreeSetCommitTsData (set commit ts for entire tree),
     TransactionIdSetCommitTs, TransactionIdGetCommitTsData,
     GetLatestCommitTsData
   - WAL records: XLOG_COMMIT_TS_ZEROPAGE, XLOG_COMMIT_TS_TRUNCATE,
     XLOG_COMMIT_TS_SETTS
   - GUC track_commit_timestamp — must be enabled for the SLRU to be active
   - oldestCommitTsXid / newestCommitTsXid in pg_control

10. MultiXact (multixact.c) — multi-transaction membership for shared row locks
    - **Two SLRU files**: pg_multixact/offsets (MultiXactId → first member
      offset, 4 bytes per multi) and pg_multixact/members
      (variable-length member arrays — TransactionId + MultiXactStatus)
    - MultiXactIdCreate, MultiXactIdExpand, MultiXactIdIsRunning,
      GetNewMultiXactId, RecordNewMultiXact, MultiXactIdGetUpdateXid,
      MultiXactGetMembers, mXactCacheGetById
    - WAL records: XLOG_MULTIXACT_ZERO_OFF_PAGE, XLOG_MULTIXACT_ZERO_MEM_PAGE,
      XLOG_MULTIXACT_CREATE_ID, XLOG_MULTIXACT_TRUNCATE_ID
    - Wraparound: SetOffsetVacuumLimit, MultiXactMemberFreezeThreshold,
      MultiXactAdvanceOldest
    - pg_control fields: nextMulti, nextMultiOffset, oldestMulti, oldestMultiDB

11. Visibility Map (visibilitymap.c)
    - 2 bits per heap page: VISIBILITYMAP_ALL_VISIBLE, VISIBILITYMAP_ALL_FROZEN
    - HEAPBLOCKS_PER_BYTE = 4, HEAPBLOCKS_PER_PAGE = (BLCKSZ - SizeOfPageHeaderData) * 4
    - visibilitymap_clear — atomic clear; called by every heap mutation
      that breaks all-visible (heap_insert, heap_update, heap_delete,
      heap_lock_tuple, heap_multi_insert)
    - visibilitymap_pin / visibilitymap_pin_ok — pin VM page **before**
      taking the heap-page lock (deadlock-avoidance protocol with vacuum)
    - visibilitymap_set — called only by vacuum / freeze, takes the
      heap-page LSN to ensure the VM page's LSN is at least as new as the
      youngest tuple it's claiming visible
    - visibilitymap_get_status, visibilitymap_count, visibilitymap_prepare_truncate
    - vm_extend, vm_readbuf — extend / read the VM relation fork
    - WAL: VM bit-set goes through XLOG_HEAP2_VISIBLE in vacuumlazy.c;
      VM bit-clear is implicit in the heap WAL record (heap_xlog_*
      decrements all-visible during redo); the VM page itself can be
      reconstructed from heap WAL at any time, so VM page writes are
      **not** WAL-logged with FPIs except when explicitly required
    - Index-only scans consult visibilitymap_get_status to skip heap
      visibility checks; if all-visible is set, the index tuple alone
      answers the query

12. Free Space Map (freespace.c, fsmpage.c)
    - Three-level FSM tree per relation, per fork (FSM_FORKNUM):
      level 0 = root (covers SlotsPerFSMPage^3 heap pages),
      level 1 = midlevel,
      level 2 = leaf (each slot = one heap page)
    - fsm_get_location, fsm_logical_to_physical, fsm_get_parent, fsm_get_child
    - fsm_search — top-down walk to find a page with at least min_cat free
      bytes; falls back to fsm_search_avail at each level
    - fsm_set_and_search — combined update + search (used after acquiring
      a buffer to record actual free space and find another candidate)
    - fsm_vacuum_page — bottom-up reconciliation to push lower-level
      maxima up to the parent
    - fsm_extend, fsm_readbuf — extend / read the FSM relation fork
    - The page-internal binary heap (fsmpage.c): each FSM page is a
      heap-ordered tree; fsm_search_avail returns the leftmost slot
      with value ≥ minvalue; fsm_set_avail bubbles updates up
    - Categories: 256 categories (1 byte) of free space, mapped to byte
      ranges by fsm_space_avail_to_cat
    - **FSM is not crash-safe by WAL** — it is a hint structure. The FSM
      is rebuilt by VACUUM (FreeSpaceMapVacuum) and is allowed to be
      out-of-date. WAL emission for FSM updates is optional and only
      triggered through XLogRecordPageWithFreeSpace for special cases
      (the heap-extension path, where the new page is WAL-logged anyway)
    - hio.c integration: RelationGetBufferForTuple uses GetPageWithFreeSpace
      to find a candidate page, locks it, and either uses it or calls
      RecordAndGetPageWithFreeSpace to update the FSM and try another
    - GetVisibilityMapPins is the deadlock-avoidance helper that pre-pins
      VM pages for all candidate heap blocks before locking heap pages
    - indexfsm.c — a simpler 0-or-1 (used / free) FSM for index relations
      (GetFreeIndexPage, RecordFreeIndexPage, RecordUsedIndexPage,
      IndexFreeSpaceMapVacuum)

13. Persistence guarantees — the integrating story
    - WAL is the durability boundary: every metadata change that must
      survive a crash either (a) emits a WAL record, or (b) is reconstructed
      from a WAL record at recovery time, or (c) is a hint that can be
      recomputed (FSM, parts of VM)
    - The buffer manager is the synchronization boundary: a metadata change
      is durable only when its WAL record is flushed to the WAL file;
      the data page itself can wait for the next checkpoint
    - pg_control is the recovery anchor: read at startup to find the redo
      pointer, the latest checkpoint, and the metadata cursors (nextXid,
      oldestXid, nextMulti, etc.); rewritten by every checkpoint via
      UpdateControlFile (atomic write + fsync)
    - Resource managers (rmgrlist.h) — the redo dispatch table; every
      metadata-relevant rmgr (XLOG, XACT, SMGR, CLOG, DBASE, TBLSPC,
      MULTIXACT, RELMAP, COMMIT_TS, HEAP2 for visibility) has a redo
      callback that replays the corresponding records
    - SLRU fsync via the sync-request queue: when a SLRU page is written,
      the page's segment is registered with `register_dirty_segment`-style
      machinery (per-SlruCtl SyncCallback) so the next checkpoint fsyncs it
    - Relmap atomicity: write_relmap_file uses temp file + fsync + rename +
      fsync of the parent directory; XLOG_RELMAP_UPDATE record carries the
      new contents so a standby can replay it
    - Checkpoint sequencing — CreateCheckPoint (xlog.c) calls
      CheckPointGuts which dispatches to:
        CheckPointCLOG, CheckPointSUBTRANS, CheckPointMultiXact,
        CheckPointPredicate, CheckPointRelationMap, CheckPointBuffers,
        CheckPointReplicationOrigin, CheckPointTwoPhase, CheckPointCommitTs
      Each flushes its dirty SLRU pages / map files
    - Startup sequencing — StartupXLOG performs (in order):
        ReadControlFile → restore checkpoint pointer → set XID/Multi cursors →
        StartupCLOG / StartupSUBTRANS / StartupMultiXact / StartupCommitTs →
        replay WAL from redo to end → TrimCLOG / TrimMultiXact (zero pages
        beyond nextXid / nextMulti) → mark consistent → open for connections
    - WAL record types touching metadata:
        XLOG_NEXTOID (in xlog.c, advancing nextOid),
        XLOG_CHECKPOINT_ONLINE / XLOG_CHECKPOINT_SHUTDOWN
        (carrying the metadata cursors in the checkpoint payload),
        XLOG_FPI / XLOG_FPI_FOR_HINT (full-page images, used for VM
        and other hint-bit-only changes that need WAL because of
        torn-page risk),
        XLOG_CLOG_ZEROPAGE / XLOG_CLOG_TRUNCATE,
        XLOG_MULTIXACT_ZERO_OFF_PAGE / _ZERO_MEM_PAGE / _CREATE_ID / _TRUNCATE_ID,
        XLOG_RELMAP_UPDATE,
        XLOG_SMGR_CREATE / XLOG_SMGR_TRUNCATE,
        XLOG_HEAP2_VISIBLE,
        XLOG_DBASE_CREATE / XLOG_DBASE_DROP,
        XLOG_TBLSPC_CREATE / XLOG_TBLSPC_DROP,
        XLOG_XACT_COMMIT / XLOG_XACT_ABORT / XLOG_XACT_PREPARE / _COMMIT_PREPARED
        (these emit per-XID CLOG state changes implicitly via xact_redo →
        TransactionIdCommitTree / TransactionIdAbortTree)
    - Hint bits: heap tuples carry HEAP_XMIN_COMMITTED / HEAP_XMAX_COMMITTED
      hint bits learned from CLOG; these are written without WAL by default
      but XLOG_FPI_FOR_HINT may be emitted under wal_log_hints / data
      checksums

14. Commit-time interaction
    - RecordTransactionCommit (xact.c): builds an xl_xact_commit record
      containing the array of subxids, the array of relations whose
      relfilenodes are dropped, the invalidation messages, and the timestamp;
      calls XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT)
    - At redo, xact_redo_commit calls TransactionIdCommitTree
      (which calls TransactionIdSetTreeStatus → CLOG update),
      ProcessCommittedInvalidationMessages (which broadcasts cache
      invalidations to all backends via sinval), and smgrDoPendingDeletes
      for dropped relations
    - This is why a single WAL record makes catalog changes visible to
      every backend on the primary AND every connected standby

15. Hooks and extension points
    - object_access_hook (objectaccess.c) — invoked at create/drop/alter
      of any object (used by sepgsql, pg_audit)
    - PostLoadCacheCallback (relcache.c) — for extensions wanting
      relcache-load notifications
    - Catcache callback registry (CacheRegisterSyscacheCallback,
      CacheRegisterRelcacheCallback) — extensions can register to be
      notified when a syscache or relcache invalidation message is processed

Generate (all files under `topic_specific_generated_docs/about_metadata/stage1/`):
- architecture_map.json with importance scores (0.0–1.0) for each symbol
- key_symbols.txt (top 50 symbols ranked by importance — the metadata
  subsystem is broad, so a larger set is appropriate)
- initial_outline.md with suggested documentation structure
- catalog_inventory.txt — every pg_catalog table with: name, Oid (when
  knowable from the .h file), header file, .dat boot data file (if any),
  C-side helper file (`pg_*.c` if any), key indexes from indexing.h,
  shared/nailed/mapped flags, brief purpose
- slru_inventory.txt — every SLRU instance with: SlruCtl name,
  on-disk directory, page-number formula (the *ToPage helper), per-page
  data layout (size of one entry × entries-per-page), checkpoint hook,
  truncation policy, redo callback
- wal_record_inventory.txt — every metadata-affecting WAL record with:
  rmgr ID, info-byte constant, payload struct, redo function, what it
  makes durable, file:line references
```

**Expected Output Check**: Verify architecture_map.json contains at least 120 symbols (the metadata domain is broad — catalog APIs, cache layers, SLRU, VM, FSM, plus the persistence stack). Verify it identifies 8+ critical paths (catalog-create, catalog-mutate, cache-invalidate, slru-write/read, vm-set, vm-clear, fsm-search, fsm-update, checkpoint, recovery-replay). Verify catalog_inventory.txt lists ≥ 60 pg_catalog tables. Verify slru_inventory.txt lists ≥ 6 SLRU instances (CLOG, SUBTRANS, MultiXactOffset, MultiXactMember, CommitTs, plus async/serial). Verify wal_record_inventory.txt lists ≥ 20 distinct metadata-affecting WAL records.

---

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for
the PostgreSQL Metadata subsystem (system catalogs, CLOG family, VM, FSM, and
their persistence guarantees).

**Source code usage for this stage**:
- For every Tier 1 symbol (importance > 0.8), read the full function
  implementation from `./src/` and annotate key logic steps.
- When documenting catalog modification, read `src/backend/catalog/heap.c`
  (heap_create_with_catalog and helpers), `src/backend/catalog/index.c`
  (index_create), and `src/backend/catalog/indexing.c` (CatalogTupleInsert and
  friends — the canonical mutation entry point) end-to-end.
- When documenting dependencies, read `src/backend/catalog/dependency.c`
  focusing on recordDependencyOn, performDeletion, findDependentObjects, and
  AcquireDeletionLock; correlate with `src/include/catalog/dependency.h`'s
  ObjectAddress / DependencyType enums.
- When documenting catalog caches, read
  `src/backend/utils/cache/catcache.c` (top file comment, then
  SearchCatCacheInternal and CatCacheInvalidate),
  `src/backend/utils/cache/syscache.c` (the cacheinfo[] table, then
  SearchSysCache1..4), and
  `src/backend/utils/cache/relcache.c` focusing on RelationIdGetRelation,
  RelationBuildDesc, formrdesc (nailed catalogs), RelationCacheInitialize,
  RelationCacheInitializePhase2, RelationCacheInitializePhase3 (the
  3-phase bootstrap), and write_relcache_init_file / load_relcache_init_file
  (`pg_internal.init`).
- When documenting cache invalidation, read
  `src/backend/utils/cache/inval.c` end-to-end and
  `src/backend/storage/ipc/sinval.c` + `sinvaladt.c` for the cross-backend
  distribution.
- When documenting the relmapper, read `src/backend/utils/cache/relmapper.c`
  end-to-end and `src/include/utils/relmapper.h`.
- When documenting the SLRU framework, read
  `src/backend/access/transam/slru.c` focusing on SimpleLruInit,
  SimpleLruReadPage, SimpleLruWritePage, SlruSelectLRUPage, the bank-lock
  partitioning, the SLRU sync-request integration, and SimpleLruTruncate.
- When documenting CLOG, read `src/backend/access/transam/clog.c`
  end-to-end and `src/include/access/clog.h`.
- When documenting subtrans, read `src/backend/access/transam/subtrans.c`
  end-to-end (it is short — ~450 lines).
- When documenting commit timestamps, read
  `src/backend/access/transam/commit_ts.c` focusing on
  TransactionIdSetCommitTs, TransactionIdGetCommitTsData, and the SLRU
  hooks (BootStrap, Startup, CheckPoint).
- When documenting MultiXact, read
  `src/backend/access/transam/multixact.c` focusing on the offsets+members
  duality, MultiXactIdCreate / MultiXactIdExpand, GetNewMultiXactId
  (the wraparound-protection logic), MultiXactGetMembers, the cache
  (mXactCacheGetById / mXactCacheGetBySet), and the recovery hooks.
- When documenting visibility map, read
  `src/backend/access/heap/visibilitymap.c` end-to-end (~600 lines), then
  cross-reference visibilitymap_set call sites in vacuumlazy.c and
  visibilitymap_clear call sites in heapam.c.
- When documenting FSM, read `src/backend/storage/freespace/freespace.c`
  end-to-end and `src/backend/storage/freespace/fsmpage.c`. Read
  `src/backend/storage/freespace/README` for the conceptual tree
  description before getting into the code. Also read the
  GetPageWithFreeSpace + GetVisibilityMapPins integration in
  `src/backend/access/heap/hio.c` (specifically RelationGetBufferForTuple).
- When documenting WAL persistence, read the redo callbacks:
  clog_redo (clog.c), commit_ts_redo (commit_ts.c), multixact_redo
  (multixact.c), relmap_redo (relmapper.c), smgr_redo (storage.c +
  catalog/storage_xlog.h), heap_xlog_visible (heapam.c — search for
  XLOG_HEAP2_VISIBLE), xact_redo / xact_redo_commit / xact_redo_abort
  (xact.c).
- When documenting checkpoints, read CreateCheckPoint and CheckPointGuts
  in `src/backend/access/transam/xlog.c`. Note the order of dispatched
  CheckPoint* calls.
- When documenting startup/recovery, read StartupXLOG in
  `src/backend/access/transam/xlogrecovery.c` for the high-level flow,
  and the corresponding Startup* / Trim* hooks in clog.c, multixact.c,
  subtrans.c, commit_ts.c.
- For data structure documentation, directly quote struct definitions
  from header files:
  - SlruCtlData, SlruSharedData (slru.h)
  - RelMapping, RelMapFile (relmapper.c)
  - CatCache, CatCList, CatCTup (catcache.h, catcache.c)
  - RelationData, the Form_pg_class fields it caches (rel.h, pg_class.h)
  - SharedInvalidationMessage (sinval.h)
  - ControlFileData (pg_control.h)
  - xl_clog_truncate, xl_multixact_create, xl_multixact_truncate,
    xl_relmap_update, xl_smgr_create, xl_smgr_truncate, xl_heap_visible,
    xl_commit_ts_set, xl_commit_ts_truncate
- Include file paths and line numbers in all source references for traceability.
- Use `grep -rn` to find all callers of key functions to document
  integration patterns accurately.

Input files (from `topic_specific_generated_docs/about_metadata/stage1/`):
- architecture_map.json
- key_symbols.txt
- initial_outline.md
- catalog_inventory.txt
- slru_inventory.txt
- wal_record_inventory.txt

Documentation Requirements:

1. For each symbol with importance > 0.8:
   - Complete API documentation (signature, parameters, return values)
   - Internal logic explanation with step-by-step walkthrough
   - Caller/callee relationships and integration patterns
   - Performance characteristics (especially: cache hit-rate sensitivity,
     SLRU bank-lock contention, FSM tree traversal cost, VM page-pin order)
   - Key invariants and assumptions, especially **persistence invariants**:
     "this function may not return until WAL is flushed", "this function
     may set hint bits without WAL", etc.

2. For each symbol with importance 0.5–0.8:
   - API documentation (signature, brief description)
   - Role within the broader metadata system
   - Key relationships to Tier 1 symbols

3. **System Catalog Inventory** (dedicated documentation for every pg_catalog
   table — group by category as in the directory layout):
   For EACH catalog, produce a standardized entry containing:
   - **Identity**: catalog name, Oid (from the .h file's
     `CATALOG(name,oid,...)` macro), header file:line, .dat file (if any)
   - **Schema**: column list with type and brief purpose; quote
     `FormData_<name>` from the header
   - **Indexes**: every system index defined for it (from indexing.h
     `DECLARE_*INDEX*` macros), with index Oid and column list
   - **Modification API**: the functions in `src/backend/catalog/pg_<name>.c`
     and / or in higher-level callers (e.g., `pg_proc.c`'s
     ProcedureCreate, the CREATE TABLE / CREATE INDEX path through
     heap.c / index.c)
   - **Cache identifier**: the `SysCacheIdentifier` enum entries that
     key into this catalog (e.g., RELOID, RELNAMENSP for pg_class)
   - **Dependencies**: which other catalogs reference its rows via
     pg_depend / pg_shdepend
   - **Storage flags**: shared (cluster-wide vs per-database), nailed
     (loaded via formrdesc), mapped (relfilenumber tracked in
     pg_filenode.map rather than pg_class.relfilenode)
   - **Bootstrap status**: whether it has a `.dat` file and what kind
     of bootstrap rows it carries (built-in functions for pg_proc,
     built-in types for pg_type, etc.)

4. **SLRU Users Catalog** (dedicated documentation for every SLRU
   instance):
   For EACH SLRU instance, produce a standardized entry containing:
   - **SlruCtl pointer**: the global variable name (e.g., `XactCtl`,
     `MultiXactOffsetCtl`, `MultiXactMemberCtl`, `SubTransCtl`,
     `CommitTsCtl`, `NotifyCtl`, `SerialSlruCtl`)
   - **On-disk directory**: the path under `$PGDATA` (e.g., `pg_xact`,
     `pg_multixact/offsets`, `pg_multixact/members`, `pg_subtrans`,
     `pg_commit_ts`, `pg_notify`, `pg_serial`)
   - **Per-page layout**: size of one entry, entries per page,
     `*_PER_PAGE` constant, the `*ToPage` function that maps a key
     (XID, MultiXactId, etc.) to a page number
   - **Page-number formula**: how a key maps to a page index
   - **Bank-lock partitioning**: which LWLock partition protects a page
     (via SimpleLruGetBankLock); typical bank count
   - **Bootstrap path**: BootStrap*, *ShmemInit
   - **Recovery path**: Startup*, Trim*
   - **Checkpoint hook**: CheckPoint*
   - **Extend / Truncate**: Extend* (called from XID/Multi advance),
     Truncate* (called from vacuum freezing)
   - **WAL records**: the redo function, and the info-byte constants
     for ZEROPAGE / TRUNCATE (and CREATE_ID for multixact, SETTS for
     commit_ts)
   - **Wraparound considerations**: how the SLRU handles XID/Multi
     wraparound; the `*Precedes` comparison function
   - **Retention**: when entries become obsolete and how they are
     truncated

5. **WAL Record Catalog for Metadata** (dedicated documentation for
   every metadata-affecting WAL record):
   For EACH record, produce:
   - rmgr ID and info-byte constant (e.g., `RM_CLOG_ID` /
     `CLOG_ZEROPAGE`)
   - Payload struct (with file:line reference)
   - Emitter (which function calls XLogInsert with this record)
   - Redo function (file:line reference)
   - What metadata it makes durable
   - Whether it carries a full-page image (XLOG_FPI / XLOG_FPI_FOR_HINT
     are universal; XLOG_HEAP2_VISIBLE conditionally)
   - Replay implications on a standby (which caches / SLRUs are
     updated, which sinval messages are propagated)

6. Required Diagrams (minimum 14 — the metadata domain spans more
   subsystems than a typical chapter):
   - End-to-end metadata persistence pipeline (catalog mutation in a
     transaction → WAL emission → checkpoint → standby replay)
   - System catalog stack: pg_catalog tables ←
     catalog/pg_*.c helpers ← catalog/heap.c + index.c + indexing.c ←
     SQL DDL command → catcache + relcache invalidations
   - Catalog cache layering: relcache built atop syscache built atop
     catcache, with the pg_internal.init shortcut
   - sinval distribution: per-backend invalidation messages flowing
     through the shared ring buffer
   - SLRU on-disk layout: SLRU_PAGES_PER_SEGMENT pages per segment file,
     bank-lock partitioning of slots
   - SLRU page-state machine (EMPTY → READING → VALID → DIRTY → WRITING)
   - CLOG page format: 2 bits per XID, 4 XIDs per byte
   - MultiXact two-SLRU duality: offsets file (4 B/multi) ↔ members file
     (5 B/member, variable-length arrays)
   - Visibility Map page format: 2 bits per heap page, ALL_VISIBLE +
     ALL_FROZEN
   - VM clear/set protocol: pin-VM-before-locking-heap deadlock-avoidance
   - FSM 3-level tree: root → midlevel → leaf, slot count per page
   - FSM page-internal binary heap (fsm_search_avail walking from the
     root of a per-page heap)
   - Heap-extension fast path through hio.c's RelationGetBufferForTuple
     ↔ FSM ↔ VM
   - pg_control + checkpoint flow: ControlFileData fields ↔ CheckPointGuts
     dispatch ↔ post-checkpoint pg_control rewrite
   - Recovery sequence: ReadControlFile → StartupXLOG → Startup* hooks
     for each SLRU → WAL replay → Trim* → consistent

7. Special Focus Areas (dedicate extra depth):
   - **Why we need a relmapper at all**: the bootstrap problem — the
     relcache for pg_class needs to know pg_class's relfilenumber, but
     pg_class's relfilenumber is itself a pg_class row. Solution: mapped
     relations live in pg_filenode.map.
   - **Atomic relmap update**: temp file + fsync + rename + fsync of
     parent directory + XLOG_RELMAP_UPDATE for crash safety on the
     primary and replication safety on the standby.
   - **The pg_internal.init shortcut**: how relcache writes a snapshot
     of nailed-and-built-from-catalog entries so subsequent backends
     skip the expensive RelationBuildDesc; what invalidates it
     (RelationCacheInitFileInvalidate) and when it is rebuilt.
   - **CatCache negative entries**: how a "no such row" is cached to
     avoid repeated catalog scans, and how invalidation of a hash bucket
     correctly purges them.
   - **sinval queue overflow → SI_RESET**: what happens when a backend
     falls so far behind that the shared ring overflows; why this is
     correct (over-invalidation is safe; under-invalidation is not).
   - **The commit-time invalidation contract**: why
     ProcessCommittedInvalidationMessages must run *after*
     TransactionIdCommitTree but before any other backend can see the
     committed state; why standbys see the same ordering.
   - **SLRU bank-lock partitioning**: the recent change from one global
     SLRU lock to per-bank locks (SLRU_BANK_BITSHIFT,
     SimpleLruGetBankLock); why bank locking improved scalability.
   - **CLOG group commit (TransactionGroupUpdateXidStatus)**: how
     concurrent committers updating the same CLOG page batch their
     updates under one bank-lock acquisition.
   - **Subtrans is not WAL-logged**: why this is safe — subtransaction
     parent links can always be reconstructed during recovery.
   - **VM bit-set is LSN-aware**: why visibilitymap_set takes an LSN
     argument and why VM page LSN must not regress past the youngest
     tuple it claims visible.
   - **XLOG_HEAP2_VISIBLE conditional FPI**: when the VM page's full
     image is included in the WAL record, when it's enough to log just
     the bit changes, and the torn-page protection contract.
   - **VM bit-clear is implicit in heap WAL**: heap_xlog_insert /
     heap_xlog_update / heap_xlog_delete / heap_xlog_lock all clear the
     VM bit during redo; the VM record itself is not separately emitted
     for clears.
   - **FSM is a hint, not a record**: why the FSM is allowed to be
     out-of-date, why it is not WAL-logged in the strict sense, and how
     vacuum re-establishes consistency via FreeSpaceMapVacuum.
   - **The hio.c deadlock-avoidance protocol**: pin VM pages for both
     candidate and current heap blocks before acquiring any heap-page
     lock, because vacuum may otherwise hold a heap-page lock while
     waiting on a VM pin.
   - **Hint bits on heap tuples**: HEAP_XMIN_COMMITTED / HEAP_XMAX_COMMITTED
     are written without WAL by default; with `wal_log_hints = on` or
     data checksums, XLOG_FPI_FOR_HINT is emitted to protect against
     torn pages.
   - **MultiXact wraparound**: why offsets and members can wrap
     independently, why SetOffsetVacuumLimit is needed, and why
     pg_control tracks oldestMulti separately from oldestXid.
   - **Catalog version (catversion.h)**: why every initdb is bound to a
     CATALOG_VERSION_NO and why a binary-incompatible catalog change
     requires an initdb.
   - **pg_control as the recovery anchor**: what happens at startup if
     pg_control is corrupt (pg_resetwal); why pg_control is the smallest
     possible target for atomic write.
   - **rmgrlist.h as the dispatch table**: how every WAL record routes
     through one rmgr; how custom_rmgr is built for extensions.

8. Source code references:
   - For each major function, include the relevant source file path
     (e.g., `src/backend/access/transam/clog.c:735`)
   - Quote critical code sections (≤20 lines) with inline annotations
   - Note important #define constants and their values
     (`CLOG_XACTS_PER_PAGE`, `MULTIXACT_OFFSETS_PER_PAGE`,
      `MULTIXACT_MEMBERS_PER_PAGE`, `COMMIT_TS_XACTS_PER_PAGE`,
      `HEAPBLOCKS_PER_BYTE`, `HEAPBLOCKS_PER_PAGE`,
      `SLRU_PAGES_PER_SEGMENT`, `SLRU_BANK_BITSHIFT`,
      `RELMAPPER_FILEMAGIC`, `RELMAP_MAX_MAPPINGS`,
      `CATALOG_VERSION_NO`, `PG_CONTROL_FILE_SIZE`)

Generate component files organized by functional area (all files under
`topic_specific_generated_docs/about_metadata/stage2/`):
- component_catalog_data_model_and_bootstrap.md  (genbki, .dat files,
                                                   formrdesc, nailed/shared/
                                                   mapped relations,
                                                   catversion, pg_control)
- component_catalog_modification_apis.md         (heap.c, index.c, indexing.c,
                                                   dependency.c, namespace.c,
                                                   storage.c, toasting.c,
                                                   aclchk.c, objectaddress.c,
                                                   the per-catalog pg_*.c
                                                   helpers)
- component_catalog_caches.md                    (catcache, syscache, relcache,
                                                   plancache, partcache,
                                                   typcache, evtcache,
                                                   relfilenumbermap)
- component_cache_invalidation.md                (inval.c, sinval.c,
                                                   sinvaladt.c, the commit-time
                                                   message-emission contract,
                                                   sinval queue overflow)
- component_relmapper.md                         (pg_filenode.map,
                                                   relmapper.c,
                                                   XLOG_RELMAP_UPDATE,
                                                   relmap_redo, atomic
                                                   write+rename+fsync)
- component_slru_framework.md                    (slru.c, SlruCtlData,
                                                   bank locking, page-state
                                                   machine, segment files,
                                                   sync-request integration,
                                                   SimpleLruInit through
                                                   SimpleLruTruncate)
- component_clog.md                              (clog.c, 2 bits/XID,
                                                   TransactionIdSetTreeStatus,
                                                   TransactionIdGetStatus,
                                                   TransactionGroupUpdateXidStatus,
                                                   ExtendCLOG, TruncateCLOG,
                                                   clog_redo, lifecycle)
- component_subtrans.md                          (subtrans.c, the not-WAL-logged
                                                   exception, reconstruction
                                                   during recovery)
- component_commit_ts.md                         (commit_ts.c, 10 B/XID,
                                                   set/get APIs, redo, GUCs)
- component_multixact.md                         (multixact.c, two-SLRU
                                                   duality, MultiXactIdCreate,
                                                   GetNewMultiXactId
                                                   wraparound logic,
                                                   MultiXactGetMembers, cache,
                                                   MultiXactMemberFreezeThreshold)
- component_visibility_map.md                    (visibilitymap.c, ALL_VISIBLE
                                                   and ALL_FROZEN bits,
                                                   set/clear protocols,
                                                   pin-before-lock,
                                                   index-only scans,
                                                   XLOG_HEAP2_VISIBLE)
- component_free_space_map.md                    (freespace.c, fsmpage.c,
                                                   3-level tree, page-internal
                                                   binary heap,
                                                   GetPageWithFreeSpace,
                                                   RecordPageWithFreeSpace,
                                                   FreeSpaceMapVacuum, hio.c
                                                   integration, indexfsm)
- component_persistence_and_wal_records.md       (rmgrlist.h-driven dispatch,
                                                   every metadata WAL record
                                                   type, redo functions,
                                                   FPI policy)
- component_checkpoints_and_recovery.md          (CreateCheckPoint /
                                                   CheckPointGuts dispatch,
                                                   pg_control update,
                                                   StartupXLOG, Startup*
                                                   hooks, Trim*, the
                                                   consistency point)
- component_hooks_and_extensibility.md           (object_access_hook,
                                                   CacheRegisterSyscacheCallback,
                                                   CacheRegisterRelcacheCallback,
                                                   custom_rmgr)
- catalog_inventory/core_relations.md            (pg_class, pg_attribute,
                                                   pg_index, pg_namespace,
                                                   pg_database, pg_tablespace,
                                                   pg_authid, pg_auth_members,
                                                   pg_am)
- catalog_inventory/type_system.md               (pg_type, pg_cast, pg_range,
                                                   pg_enum, pg_collation,
                                                   pg_conversion)
- catalog_inventory/functions_and_operators.md   (pg_proc, pg_aggregate,
                                                   pg_operator, pg_amop,
                                                   pg_amproc, pg_opclass,
                                                   pg_opfamily, pg_language)
- catalog_inventory/constraints_and_dependencies.md (pg_constraint,
                                                      pg_depend, pg_shdepend,
                                                      pg_attrdef, pg_inherits)
- catalog_inventory/partitioning.md              (pg_partitioned_table,
                                                   pg_inherits, pg_class
                                                   partition flags)
- catalog_inventory/statistics.md                (pg_statistic,
                                                   pg_statistic_ext,
                                                   pg_statistic_ext_data)
- catalog_inventory/access_control.md            (pg_authid, pg_auth_members,
                                                   pg_database, pg_tablespace,
                                                   pg_default_acl,
                                                   pg_init_privs, pg_policy,
                                                   pg_seclabel, pg_shseclabel,
                                                   pg_parameter_acl)
- catalog_inventory/replication_and_publication.md (pg_publication,
                                                     pg_publication_rel,
                                                     pg_publication_namespace,
                                                     pg_subscription,
                                                     pg_subscription_rel,
                                                     pg_replication_origin)
- catalog_inventory/triggers_and_rewrite.md      (pg_trigger,
                                                   pg_event_trigger,
                                                   pg_rewrite)
- catalog_inventory/extensions_and_fdw.md        (pg_extension,
                                                   pg_foreign_data_wrapper,
                                                   pg_foreign_server,
                                                   pg_foreign_table,
                                                   pg_user_mapping,
                                                   pg_transform)
- catalog_inventory/text_search.md               (pg_ts_config,
                                                   pg_ts_config_map,
                                                   pg_ts_dict, pg_ts_parser,
                                                   pg_ts_template)
- catalog_inventory/misc.md                      (pg_largeobject,
                                                   pg_largeobject_metadata,
                                                   pg_db_role_setting,
                                                   pg_description,
                                                   pg_shdescription)
- slru_users_catalog/clog.md                     (pg_xact, 2 b/XID)
- slru_users_catalog/subtrans.md                 (pg_subtrans, 4 B/XID,
                                                   not-WAL-logged)
- slru_users_catalog/multixact_offsets.md        (pg_multixact/offsets,
                                                   4 B/multi)
- slru_users_catalog/multixact_members.md        (pg_multixact/members,
                                                   5 B/member entry)
- slru_users_catalog/commit_ts.md                (pg_commit_ts, 10 B/XID)
- slru_users_catalog/other_slru_users.md         (pg_notify via async.c,
                                                   pg_serial via predicate.c
                                                   — briefly, since they are
                                                   tangential to the metadata
                                                   theme)
- wal_record_catalog/clog_records.md             (CLOG_ZEROPAGE,
                                                   CLOG_TRUNCATE)
- wal_record_catalog/multixact_records.md        (XLOG_MULTIXACT_*)
- wal_record_catalog/relmap_records.md           (XLOG_RELMAP_UPDATE)
- wal_record_catalog/storage_smgr_records.md     (XLOG_SMGR_CREATE,
                                                   XLOG_SMGR_TRUNCATE)
- wal_record_catalog/heap_visible_records.md     (XLOG_HEAP2_VISIBLE)
- wal_record_catalog/commit_ts_records.md        (COMMIT_TS_*)
- wal_record_catalog/nextoid_and_checkpoint_records.md (XLOG_NEXTOID,
                                                         XLOG_CHECKPOINT_*,
                                                         XLOG_FPI,
                                                         XLOG_FPI_FOR_HINT)
- wal_record_catalog/dbase_and_tblspc_records.md (XLOG_DBASE_*,
                                                   XLOG_TBLSPC_*)
- diagrams/*.mermaid                              (under
                                                    `topic_specific_generated_docs/about_metadata/stage2/diagrams/`)
```

**Expected Output Check**: Ensure all Tier 1 symbols (importance > 0.8) have detailed documentation with source references. Verify minimum 14 diagrams are generated. Verify every pg_catalog table from catalog_inventory.txt has a catalog-inventory entry. Verify every SLRU instance from slru_inventory.txt has a slru_users_catalog entry. Verify every WAL record from wal_record_inventory.txt has a wal_record_catalog entry.

---

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the integration-optimizer subagent:

```
Integrate all documentation components into a cohesive, professional technical
document for the PostgreSQL Metadata subsystem.

**Source code verification for this stage**:
- Before finalizing, spot-check at least 25 critical function signatures and
  struct definitions against `./src/` to ensure accuracy (more than usual due
  to the large number of subsystems and catalogs).
- Verify that all quoted code snippets in the documentation match the actual
  source.
- Confirm file paths referenced in the documentation are valid:
  `ls ./src/path/to/file.c`.
- Cross-check every catalog_inventory entry: verify the header file exists,
  the .dat file exists when claimed, and the helper file
  `src/backend/catalog/pg_<name>.c` exists when referenced.
- Cross-check every slru_users_catalog entry: verify the SimpleLruInit call
  exists for the named SlruCtl with the named on-disk directory.
- Cross-check every wal_record_catalog entry: verify the info-byte
  constant exists in the named header and the redo function exists in the
  named .c file.

Input files (from `topic_specific_generated_docs/about_metadata/stage2/`):
- All component_*.md files from Stage 2
- All catalog_inventory/*.md files
- All slru_users_catalog/*.md files
- All wal_record_catalog/*.md files
- All diagrams/*.mermaid files
- architecture_map.json for reference (from
  `topic_specific_generated_docs/about_metadata/stage1/`)
- catalog_inventory.txt, slru_inventory.txt, wal_record_inventory.txt
  for reference

Integration Requirements:

1. Document Structure:
   - Executive Summary (1 page): The metadata subsystem's role as the
     "self-describing" layer of the database; the four data domains
     (catalogs, commit-log family, VM, FSM); the unifying persistence
     story (WAL + checkpoint + pg_control as recovery anchor); the
     trade-off between **strict durability** (catalog rows, CLOG, MultiXact,
     relmap — full WAL) and **hint-style metadata** (FSM, hint bits — best
     effort, recomputable).
   - Architecture Overview: System-wide perspective with a main structural
     diagram showing the four domains and the WAL/checkpoint/pg_control
     spine that ties them together.
   - Core Components (organized by data domain, then by function within
     each domain):
     a. Catalog Data Model and Bootstrap — genbki, .dat files,
        catversion, pg_control, formrdesc, nailed/shared/mapped
     b. Catalog Modification APIs — heap.c, index.c, indexing.c,
        dependency.c, namespace.c, storage.c, toasting.c, aclchk.c,
        objectaddress.c, per-catalog pg_*.c helpers
     c. Catalog Caches — catcache → syscache → relcache, plus auxiliary
        caches (plancache, partcache, typcache, evtcache, attoptcache,
        spccache, ts_cache, relfilenumbermap)
     d. Cache Invalidation — inval.c, sinval.c, sinvaladt.c, the
        commit-time invalidation message contract
     e. Relmapper — pg_filenode.map, relmapper.c, XLOG_RELMAP_UPDATE,
        the bootstrap-circularity solution
     f. SLRU Framework — slru.c, bank locking, page-state machine,
        segment files, sync requests
     g. CLOG — commit/abort status, group commit, lifecycle
     h. SUBTRANS — subtransaction parent linkage (the not-WAL-logged
        exception)
     i. Commit Timestamps — commit_ts.c, GUC-gated lifecycle
     j. MultiXact — two-SLRU duality, wraparound, freeze threshold
     k. Visibility Map — ALL_VISIBLE / ALL_FROZEN bits, set/clear
        protocols, index-only scan integration
     l. Free Space Map — 3-level tree, page-internal heap, hio.c fast
        path, hint nature
     m. Persistence and WAL Records — every metadata-affecting WAL
        record, FPI policy
     n. Checkpoints and Recovery — CreateCheckPoint dispatch order,
        StartupXLOG sequence, Trim* hooks
     o. Hooks and Extensibility — object_access_hook, cache callbacks,
        custom_rmgr
   - **System Catalog Inventory** (dedicated chapter):
     A comprehensive catalog of every pg_catalog table, organized by
     category (core relations, type system, functions and operators,
     constraints and dependencies, partitioning, statistics, access
     control, replication and publication, triggers and rewrite,
     extensions and FDW, text search, misc). Each entry follows the
     standardized template (identity, schema, indexes, modification API,
     cache identifier, dependencies, storage flags, bootstrap status).
   - **SLRU Users Catalog** (dedicated chapter):
     A comprehensive catalog of every SLRU-backed metadata file. Each
     entry follows the standardized template (SlruCtl pointer, on-disk
     directory, per-page layout, page-number formula, bank-lock
     partitioning, bootstrap path, recovery path, checkpoint hook,
     extend / truncate, WAL records, wraparound, retention).
   - **Metadata WAL Record Catalog** (dedicated chapter):
     A comprehensive catalog of every WAL record affecting metadata.
     Each entry follows the standardized template (rmgr ID, info byte,
     payload struct, emitter, redo function, what it makes durable,
     FPI policy, standby implications).
   - Deep Dives: Complex topics including:
     - The relmapper bootstrap-circularity solution
     - The pg_internal.init shortcut and its invalidation
     - CatCache negative entries and their hash-bucket invalidation
     - sinval queue overflow → SI_RESET correctness argument
     - The commit-time invalidation contract — ordering with
       TransactionIdCommitTree
     - SLRU bank-lock partitioning and the move from one global lock
     - CLOG group commit (TransactionGroupUpdateXidStatus)
     - Why subtrans is not WAL-logged
     - VM bit-set LSN-aware invariant
     - XLOG_HEAP2_VISIBLE conditional FPI
     - VM bit-clear is implicit in heap WAL
     - FSM as a hint, not a record — the recomputability argument
     - hio.c deadlock-avoidance protocol (pin-VM-before-lock-heap)
     - Heap hint bits and `wal_log_hints`
     - MultiXact wraparound (offsets vs members)
     - catversion.h binding and binary-incompatible catalog changes
     - pg_control as the recovery anchor
     - rmgrlist.h-driven dispatch and custom_rmgr
   - Appendices:
     - Symbol index (alphabetical, with source file locations)
     - Glossary of metadata terminology (catalog, syscache, relcache,
       sinval, SLRU, CLOG, MultiXact, VM, FSM, relmap, hint bit, FPI,
       redo, checkpoint, wraparound, frozen, all-visible, ...)
     - Key data structure reference (RelationData, FormData_pg_class,
       SlruCtlData, SlruSharedData, RelMapping, RelMapFile,
       SharedInvalidationMessage, CatCache, CatCList,
       ControlFileData, xl_clog_truncate, xl_multixact_create,
       xl_multixact_truncate, xl_relmap_update, xl_smgr_create,
       xl_smgr_truncate, xl_heap_visible, xl_commit_ts_set,
       xl_commit_ts_truncate)
     - pg_catalog quick-reference table (Name → Oid → header file →
       .dat file → SysCacheIdentifier(s) → key indexes — one row per
       catalog)
     - SLRU quick-reference table (SlruCtl → directory → entry size →
       entries per page → page formula → checkpoint hook → redo function
       — one row per SLRU instance)
     - WAL record quick-reference table (rmgr → info byte → payload →
       emitter → redo function — one row per record)
     - On-disk file map ($PGDATA layout): global/pg_control,
       global/pg_filenode.map, base/$DBOID/pg_filenode.map,
       base/$DBOID/pg_internal.init, pg_xact/, pg_multixact/{offsets,members}/,
       pg_subtrans/, pg_commit_ts/, pg_notify/, pg_serial/, pg_xlog or
       pg_wal/, pg_twophase/, pg_logical/, pg_replslot/, pg_stat_tmp/,
       pg_dynshmem/
     - Key GUC parameters (track_commit_timestamp, wal_log_hints,
       max_wal_senders, hot_standby, recovery_target_*,
       autovacuum_freeze_max_age, vacuum_freeze_table_age,
       vacuum_freeze_min_age, vacuum_multixact_freeze_table_age,
       vacuum_multixact_freeze_min_age, autovacuum_multixact_freeze_max_age,
       transaction_buffers, multixact_offset_buffers,
       multixact_member_buffers, subtransaction_buffers,
       commit_timestamp_buffers, notify_buffers, serializable_buffers)
     - Further reading: src/backend/access/transam/README,
       src/backend/storage/freespace/README,
       src/backend/access/heap/README.HOT,
       src/backend/utils/cache/syscache.c top comment,
       src/include/catalog/pg_control.h top comment,
       relevant PostgreSQL wiki and docs pages

2. Enhancement Tasks:
   - Generate comprehensive cross-references between sections (e.g., the
     CLOG component links to the SLRU framework component and to the
     CLOG SLRU users catalog and to the CLOG WAL record catalog)
   - Eliminate redundancy between component chapters and the catalogs —
     the catalogs focus on per-instance specifics; the chapters provide
     cross-cutting concepts
   - Standardize terminology (prefer PostgreSQL implementation terms:
     "RelOptInfo" → "Relation" (or RelationData) for the relcache entry,
     "system catalog" not "data dictionary", "SLRU" not "circular log",
     "CLOG" not "commit log file", "Visibility Map" / "VM" not "vis map",
     "Free Space Map" / "FSM" not "free space file", "relmap" /
     "relmapper" not "filenode map", "hint bit" not "shortcut bit",
     "redo" not "replay" (though "WAL replay" is also acceptable when
     describing the standby path), "FPI" / "full-page image" not
     "full-page write" (the latter is the GUC name), "checkpoint" not
     "sync point")
   - Add navigation aids (Table of Contents, section breadcrumbs, next/prev
     links)
   - Ensure consistent diagram style and labeling across all Mermaid
     diagrams
   - For the catalog_inventory: ensure every entry references at least one
     SQL example (e.g., a query against the catalog or the DDL command
     that creates rows in it)
   - For the wal_record_catalog: ensure every entry shows the redo path
     (which SLRU page or which buffer-manager block is mutated)

3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented somewhere in the
     output
   - Verify all pg_catalog tables from catalog_inventory.txt have entries
   - Verify every SLRU from slru_inventory.txt has an entry
   - Verify every WAL record from wal_record_inventory.txt has an entry
   - Ensure logical flow: high-level concepts → architecture →
     implementation details → catalog reference
   - Validate all internal cross-reference links
   - Check all Mermaid diagrams render correctly (valid syntax)
   - Confirm code examples and source references match actual PostgreSQL
     source
   - Flag any remaining ambiguities or areas needing community review

4. Output Organization:
   Since total size will likely exceed 4500 lines (larger than usual due
   to the four-domain catalog + SLRU catalog + WAL record catalog + the
   persistence integration story):
   - Split into logical modules with clear boundaries
   - Create index.md as the navigation hub linking all modules
   - Maintain coherent reading experience with "Prerequisites" and "Next"
     notes per module
   - Each module should be self-contained enough for targeted reading
   - **All final output files must be written under
     `topic_specific_generated_docs/about_metadata/final/`**
   - **Consolidated diagrams must be copied to
     `topic_specific_generated_docs/about_metadata/diagrams/`**

   Module structure (all under `topic_specific_generated_docs/about_metadata/final/`):
   - index.md                                   (navigation hub, reading guide)
   - 01_executive_summary.md                    (overview for newcomers)
   - 02_architecture_overview.md                (system-wide perspective,
                                                  the WAL/checkpoint/pg_control
                                                  spine)
   - 03_catalog_data_model_and_bootstrap.md     (genbki, .dat, formrdesc,
                                                  nailed/shared/mapped,
                                                  catversion, pg_control)
   - 04_catalog_modification_apis.md            (heap.c, index.c, indexing.c,
                                                  dependency.c, namespace.c,
                                                  storage.c, toasting.c,
                                                  aclchk.c, objectaddress.c,
                                                  per-catalog helpers)
   - 05_catalog_caches.md                       (catcache → syscache →
                                                  relcache + auxiliary caches)
   - 06_cache_invalidation.md                   (inval, sinval, sinvaladt,
                                                  commit-time message contract)
   - 07_relmapper.md                            (pg_filenode.map, relmapper.c,
                                                  XLOG_RELMAP_UPDATE)
   - 08_slru_framework.md                       (slru.c, bank locking,
                                                  page-state machine,
                                                  sync requests)
   - 09_clog.md                                 (commit/abort status,
                                                  group commit, lifecycle)
   - 10_subtrans.md                             (subtransaction parents,
                                                  not-WAL-logged exception)
   - 11_commit_timestamps.md                    (commit_ts.c, GUC, lifecycle)
   - 12_multixact.md                            (two-SLRU duality, wraparound,
                                                  freeze threshold)
   - 13_visibility_map.md                       (ALL_VISIBLE / ALL_FROZEN,
                                                  set/clear protocols, IOS)
   - 14_free_space_map.md                       (3-level tree, page-internal
                                                  heap, hio.c, hint nature)
   - 15_persistence_and_wal_records.md          (rmgrlist-driven dispatch,
                                                  every metadata WAL record,
                                                  FPI policy)
   - 16_checkpoints_and_recovery.md             (CreateCheckPoint dispatch,
                                                  StartupXLOG, Trim* hooks)
   - 17_hooks_and_extensibility.md              (object_access_hook,
                                                  cache callbacks, custom_rmgr)
   - 18_catalog_inventory.md                    (every pg_catalog table —
                                                  detailed catalog)
   - 19_slru_users_catalog.md                   (every SLRU instance —
                                                  detailed catalog)
   - 20_wal_record_catalog.md                   (every metadata-affecting
                                                  WAL record — detailed catalog)
   - 21_deep_dives.md                           (relmapper bootstrap,
                                                  pg_internal.init shortcut,
                                                  catcache negatives,
                                                  sinval overflow,
                                                  commit-time inval contract,
                                                  SLRU bank locks,
                                                  CLOG group commit,
                                                  subtrans not WAL-logged,
                                                  VM bit-set LSN invariant,
                                                  XLOG_HEAP2_VISIBLE FPI,
                                                  VM clear-implicit-in-heap-WAL,
                                                  FSM as a hint,
                                                  hio.c deadlock avoidance,
                                                  hint bits and wal_log_hints,
                                                  multixact wraparound,
                                                  catversion binding,
                                                  pg_control as anchor,
                                                  rmgrlist dispatch)
   - appendix_symbol_index.md                  (alphabetical symbol reference)
   - appendix_glossary.md                      (metadata terminology)
   - appendix_data_structures.md               (key struct definitions:
                                                 RelationData, FormData_pg_class,
                                                 SlruCtlData, SlruSharedData,
                                                 RelMapping, RelMapFile,
                                                 SharedInvalidationMessage,
                                                 CatCache, CatCList,
                                                 ControlFileData, every xl_*
                                                 payload struct)
   - appendix_pg_catalog_quick_reference.md    (one row per catalog table)
   - appendix_slru_quick_reference.md          (one row per SLRU instance)
   - appendix_wal_record_quick_reference.md    (one row per metadata WAL
                                                 record)
   - appendix_pgdata_layout.md                 ($PGDATA on-disk file map
                                                 with what each directory
                                                 contains and which subsystem
                                                 manages it)
   - appendix_guc_parameters.md                (every metadata-relevant GUC)

5. Additional Deliverables (also under
   `topic_specific_generated_docs/about_metadata/final/`):
   - metadata_quick_reference.md   (3-page summary: the four domains,
                                     WAL/checkpoint/pg_control spine,
                                     key APIs (CatalogTupleInsert,
                                     SearchSysCache1, RelationIdGetRelation,
                                     visibilitymap_set/clear,
                                     GetPageWithFreeSpace, RecordPageWithFreeSpace,
                                     TransactionIdSetTreeStatus,
                                     TransactionIdGetStatus,
                                     MultiXactIdCreate), checkpoint dispatch
                                     order, recovery sequence, key GUCs,
                                     diagnostics (pg_visibility, pg_freespacemap,
                                     pg_xact_status, pg_get_multixact_members))
   - metadata_api_reference.md     (function signatures grouped by subsystem,
                                     with brief descriptions)
   - quality_report.md             (coverage metrics: % of key_symbols
                                     documented, % of pg_catalog tables
                                     cataloged, % of SLRU instances cataloged,
                                     % of WAL records cataloged, diagram count,
                                     known gaps, improvement suggestions)
```

**Expected Output Check**: Verify professional documentation quality, complete symbol coverage (>80%), complete pg_catalog inventory coverage (100% of catalog_inventory.txt entries), complete SLRU users catalog coverage (100% of slru_inventory.txt entries), complete WAL record catalog coverage (100% of wal_record_inventory.txt entries), and coherent navigation structure.

---

## Orchestration Rules

### Execution Flow
1. **Before Stage 1**: Activate the project venv and create the output directory tree:
   ```bash
   source venv/bin/activate
   mkdir -p topic_specific_generated_docs/about_metadata/{stage1,stage2/diagrams,stage2/catalog_inventory,stage2/slru_users_catalog,stage2/wal_record_catalog,final,diagrams}
   ```
2. Execute each stage sequentially — do not proceed until the previous stage completes successfully
3. Capture all output files from each subagent into the appropriate subdirectory under `topic_specific_generated_docs/about_metadata/`
4. Validate expected outputs before proceeding to the next stage
5. Report progress after each stage

### Source Tree Primacy
- The local `./src/` directory is the **single source of truth**.
- `src/backend/access/transam/README` is the authoritative conceptual document for the SLRU/CLOG/MultiXact/CommitTs/transaction-WAL story — read it before relying on any synthesized description.
- `src/backend/storage/freespace/README` is the authoritative document for the FSM tree.
- `src/include/catalog/pg_control.h` (top file comment) is the authoritative description of the cluster control file.
- Subagents should use `./src/` for structural exploration (file layout, neighboring functions, header inclusions).
- All generated documentation must include verifiable source file paths relative to `./src/`.

### Error Handling
- **MCP tool failure**: If `pg_*` MCP tools fail (e.g., "snode_module.py not found" or "duckdb module missing"), fall back to direct source reading via `Read`, `Grep`, and `Bash` tools. Do not block on MCP availability.
- **Subagent failure**: Retry once with modified parameters (e.g., reduce scope), then proceed with partial results and document gaps
- **Missing expected files**: Log warning, attempt recovery using available data, note in quality_report.md
- **Context limit approaching**: Save progress checkpoint, split remaining work into smaller focused chunks, resume from checkpoint. **For the catalogs**: if context limits are hit, process pg_catalog inventory in batches (core relations + type system first, then the rest), and similarly for SLRU users and WAL records.
- **Symbol not found**: Log missing symbol, attempt alternative names (e.g., with/without `pg_` prefix, with/without `_redo` suffix), continue with available data

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, coverage %, pg_catalog tables cataloged, SLRU instances cataloged, WAL records cataloged>
Issues encountered: <any warnings or partial failures>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical-path symbols are documented:
   `CatalogTupleInsert`, `CatalogTupleUpdate`, `CatalogTupleDelete`,
   `heap_create_with_catalog`, `heap_drop_with_catalog`,
   `index_create`, `index_drop`,
   `recordDependencyOn`, `performDeletion`,
   `RangeVarGetRelid`, `LookupExplicitNamespace`,
   `RelationCreateStorage`, `RelationDropStorage`, `log_smgrcreate`,
   `SearchSysCache1`, `SearchSysCache2`, `ReleaseSysCache`,
   `SearchCatCacheInternal`, `CatCacheInvalidate`,
   `RelationIdGetRelation`, `RelationClose`, `RelationBuildDesc`,
   `formrdesc`, `RelationCacheInitializePhase3`,
   `write_relcache_init_file`, `RelationCacheInitFileInvalidate`,
   `CacheInvalidateRelcache`, `CacheInvalidateHeapTuple`,
   `RegisterCatcacheInvalidation`, `xactGetCommittedInvalidationMessages`,
   `ProcessCommittedInvalidationMessages`, `AtEOXact_Inval`,
   `SendSharedInvalidMessages`, `ReceiveSharedInvalidMessages`,
   `SIInsertDataEntries`, `SIGetDataEntries`,
   `RelationMapOidToFilenumber`, `RelationMapUpdateMap`,
   `write_relmap_file`, `load_relmap_file`, `relmap_redo`,
   `SimpleLruInit`, `SimpleLruReadPage`, `SimpleLruWritePage`,
   `SimpleLruWriteAll`, `SimpleLruTruncate`, `SlruSelectLRUPage`,
   `TransactionIdSetTreeStatus`, `TransactionIdGetStatus`,
   `TransactionGroupUpdateXidStatus`, `ExtendCLOG`, `TruncateCLOG`,
   `CheckPointCLOG`, `clog_redo`,
   `SubTransSetParent`, `SubTransGetTopmostTransaction`,
   `TransactionIdSetCommitTs`, `TransactionIdGetCommitTsData`,
   `commit_ts_redo`,
   `MultiXactIdCreate`, `MultiXactIdExpand`, `GetNewMultiXactId`,
   `MultiXactGetMembers`, `multixact_redo`,
   `MultiXactAdvanceOldest`, `SetOffsetVacuumLimit`,
   `visibilitymap_clear`, `visibilitymap_pin`, `visibilitymap_set`,
   `visibilitymap_get_status`, `visibilitymap_count`,
   `vm_readbuf`, `vm_extend`, `heap_xlog_visible`,
   `GetPageWithFreeSpace`, `RecordPageWithFreeSpace`,
   `RecordAndGetPageWithFreeSpace`, `FreeSpaceMapVacuum`,
   `fsm_search`, `fsm_set_and_search`, `fsm_vacuum_page`,
   `fsm_search_avail`, `fsm_set_avail`,
   `RelationGetBufferForTuple`, `GetVisibilityMapPins`,
   `RecordTransactionCommit`, `RecordTransactionAbort`,
   `xact_redo_commit`, `xact_redo_abort`,
   `CreateCheckPoint`, `CheckPointGuts`, `UpdateControlFile`,
   `StartupXLOG`, `ReadControlFile`,
   `StartupCLOG`, `TrimCLOG`, `StartupMultiXact`, `TrimMultiXact`,
   `XLogInsert`, `XLogFlush`
2. Verify every pg_catalog table has a catalog_inventory entry (target = 100%)
3. Verify every SLRU instance has a slru_users_catalog entry (target = 100%)
4. Verify every metadata-affecting WAL record has a wal_record_catalog entry (target = 100%)
5. Count and list all generated diagrams (must be ≥ 14)
6. Check total documentation coverage against key_symbols.txt (target > 80%)
7. Ensure no broken cross-references or unresolved TODO markers remain
8. Confirm file organization follows the specified module structure
9. Validate all Mermaid diagram syntax

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Comprehensive metadata documentation generated covering all 15 functional areas (catalog data model, catalog modification, catalog caches, cache invalidation, relmapper, SLRU framework, CLOG, SUBTRANS, commit timestamps, MultiXact, VM, FSM, persistence/WAL records, checkpoints/recovery, hooks)
- [ ] Complete pg_catalog inventory covering 100% of system catalog tables with standardized entries
- [ ] Complete SLRU users catalog covering 100% of SLRU-backed metadata files
- [ ] Complete WAL record catalog covering 100% of metadata-affecting WAL records
- [ ] Minimum 14 technical diagrams included and rendering correctly
- [ ] quality_report.md shows > 80% symbol coverage, 100% pg_catalog inventory coverage, 100% SLRU catalog coverage, and 100% WAL record catalog coverage
- [ ] Documentation is organized into navigable modules with index.md
- [ ] Both high-level overview (suitable for newcomers) and deep implementation details (suitable for PostgreSQL contributors) are present
- [ ] The unifying persistence story (WAL + checkpoint + pg_control as recovery anchor) is clearly explained and integrates all four data domains
- [ ] Quick reference and API reference supplements are generated

---

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages — proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL Metadata Documentation Generation - Stage 1: Architecture Analysis"
