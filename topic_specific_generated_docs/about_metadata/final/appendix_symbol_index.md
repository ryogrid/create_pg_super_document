# Appendix — Symbol Index

[Up: index.md](index.md)  |  [Prev: 21 Deep Dives](21_deep_dives.md)  |  [Next: appendix_glossary.md](appendix_glossary.md)

Alphabetical reference for every symbol mentioned in this document.
The "Where" column links to the chapter that documents the symbol;
the "Source" column gives the canonical file:line.

## A

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `AcceptInvalidationMessages`                  | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`            |
| `AcquireDeletionLock`                         | [04](04_catalog_modification_apis.md) | `src/backend/catalog/dependency.c`           |
| `ActivateCommitTs`                            | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c`     |
| `AddCatcacheInvalidationMessage`              | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`            |
| `AddNewAttributeTuples`                       | [04](04_catalog_modification_apis.md) | `src/backend/catalog/heap.c`                  |
| `AddNewRelationTuple`                         | [04](04_catalog_modification_apis.md) | `src/backend/catalog/heap.c`                  |
| `AddRelcacheInvalidationMessage`              | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`            |
| `AddSnapshotInvalidationMessage`              | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`            |
| `AdvanceOldestClogXid`                        | [09](09_clog.md), [20](20_wal_record_catalog.md) | `src/backend/access/transam/clog.c` |
| `AggregateCreate`                             | [03](03_catalog_data_model_and_bootstrap.md), [18](18_catalog_inventory.md) | `src/backend/catalog/pg_aggregate.c` |
| `AllocateRelationDesc`                        | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c`         |
| `AsyncQueueEntry`                             | [19](19_slru_users_catalog.md) | `src/backend/commands/async.c`                |
| `AtCCI_RelationMap`                           | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`        |
| `AtEOSubXact_Inval`                           | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`            |
| `AtEOXact_Inval`                              | [06](06_cache_invalidation.md), [15](15_persistence_and_wal_records.md) | `src/backend/utils/cache/inval.c:1026`       |
| `AtEOXact_RelationMap`                        | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`        |
| `AuthIdRelationId`                            | [03](03_catalog_data_model_and_bootstrap.md), [18](18_catalog_inventory.md) | `src/include/catalog/pg_authid.h`             |

## B

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `bank_locks`                                  | [08](08_slru_framework.md) | `src/include/access/slru.h`                  |
| `BootStrapCLOG`                               | [09](09_clog.md) | `src/backend/access/transam/clog.c`          |
| `BootStrapCommitTs`                           | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c`     |
| `BootStrapMultiXact`                          | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`      |
| `BootStrapSUBTRANS`                           | [10](10_subtrans.md) | `src/backend/access/transam/subtrans.c`       |
| `BootStrapXLOG`                               | [03](03_catalog_data_model_and_bootstrap.md) | `src/backend/access/transam/xlog.c`           |
| `buffer_locks`                                | [08](08_slru_framework.md) | `src/include/access/slru.h`                   |

## C

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `CacheInvalidateCatalog`                      | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`             |
| `CacheInvalidateHeapTuple`                    | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c:1207`        |
| `CacheInvalidateHeapTupleByRelid`             | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`             |
| `CacheInvalidateRelcache`                     | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c:1363`        |
| `CacheInvalidateRelcacheAll`                  | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c:1387`        |
| `CacheInvalidateRelcacheByRelid`              | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c:1422`        |
| `CacheInvalidateRelmap`                       | [06](06_cache_invalidation.md), [07](07_relmapper.md) | `src/backend/utils/cache/inval.c`             |
| `CacheInvalidateSmgr`                         | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`             |
| `CacheRegisterRelcacheCallback`               | [17](17_hooks_and_extensibility.md) | `src/backend/utils/cache/inval.c:1561`        |
| `CacheRegisterSyscacheCallback`               | [17](17_hooks_and_extensibility.md) | `src/backend/utils/cache/inval.c:1519`        |
| `CallRelcacheCallbacks`                       | [17](17_hooks_and_extensibility.md) | `src/backend/utils/cache/inval.c`              |
| `CallSyscacheCallbacks`                       | [17](17_hooks_and_extensibility.md) | `src/backend/utils/cache/inval.c`              |
| `CATALOG_VERSION_NO`                          | [03](03_catalog_data_model_and_bootstrap.md), [21](21_deep_dives.md) | `src/include/catalog/catversion.h`            |
| `CatalogIndexInsert`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c`               |
| `CatalogTupleDelete`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c:365`           |
| `CatalogTupleInsert`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c:233`           |
| `CatalogTupleUpdate`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c:313`           |
| `CatalogTupleInsertWithInfo`                  | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c`               |
| `CatalogTuplesMultiInsertWithInfo`            | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c`               |
| `CatCache`                                    | [05](05_catalog_caches.md) | `src/include/utils/catcache.h`                  |
| `CatCacheInvalidate`                          | [05](05_catalog_caches.md), [21](21_deep_dives.md) | `src/backend/utils/cache/catcache.c:625`       |
| `CatCList`                                    | [05](05_catalog_caches.md) | `src/include/utils/catcache.h`                  |
| `CatCTup`                                     | [05](05_catalog_caches.md) | `src/include/utils/catcache.h`                  |
| `CatalogOpenIndexes`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c`               |
| `CatalogCloseIndexes`                         | [04](04_catalog_modification_apis.md) | `src/backend/catalog/indexing.c`               |
| `CatalogCacheInitializeCache`                 | [05](05_catalog_caches.md) | `src/backend/utils/cache/catcache.c`            |
| `CheckPoint` (struct)                         | [03](03_catalog_data_model_and_bootstrap.md), [16](16_checkpoints_and_recovery.md) | `src/include/catalog/pg_control.h:35`         |
| `CheckPointBuffers`                           | [16](16_checkpoints_and_recovery.md) | `src/backend/storage/buffer/bufmgr.c`         |
| `CheckPointCLOG`                              | [09](09_clog.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/clog.c:937`       |
| `CheckPointCommitTs`                          | [11](11_commit_timestamps.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/commit_ts.c`      |
| `CheckPointGuts`                              | [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/xlog.c:7504`       |
| `CheckPointMultiXact`                         | [12](12_multixact.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/multixact.c`       |
| `CheckPointPredicate`                         | [16](16_checkpoints_and_recovery.md), [19](19_slru_users_catalog.md) | `src/backend/storage/lmgr/predicate.c`         |
| `CheckPointRelationMap`                       | [07](07_relmapper.md), [16](16_checkpoints_and_recovery.md) | `src/backend/utils/cache/relmapper.c`          |
| `CheckPointSUBTRANS`                          | [10](10_subtrans.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/subtrans.c`        |
| `CheckPointTwoPhase`                          | [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/twophase.c`        |
| `clog_redo`                                   | [09](09_clog.md), [20](20_wal_record_catalog.md) | `src/backend/access/transam/clog.c:1107`       |
| `CLOG_LSNS_PER_PAGE`                          | [09](09_clog.md) | `src/include/access/clog.h`                    |
| `CLOG_XACTS_PER_PAGE`                         | [09](09_clog.md) | `src/include/access/clog.h`                    |
| `COMMIT_TS_XACTS_PER_PAGE`                    | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c`        |
| `CommandEndInvalidationMessages`              | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`              |
| `CommitTimestampEntry`                        | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c`        |
| `commit_ts_redo`                              | [11](11_commit_timestamps.md), [20](20_wal_record_catalog.md) | `src/backend/access/transam/commit_ts.c:1023`   |
| `CommitTsCtl`                                 | [11](11_commit_timestamps.md), [19](19_slru_users_catalog.md) | `src/backend/access/transam/commit_ts.c`        |
| `ControlFileData`                             | [03](03_catalog_data_model_and_bootstrap.md), [16](16_checkpoints_and_recovery.md) | `src/include/catalog/pg_control.h:104`          |
| `CreateCheckPoint`                            | [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/xlog.c:6863`        |
| `CreateConstraintEntry`                       | [04](04_catalog_modification_apis.md), [18](18_catalog_inventory.md) | `src/backend/catalog/pg_constraint.c`            |
| `CreateRestartPoint`                          | [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/xlog.c`              |

## D

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `dbase_redo`                                  | [20](20_wal_record_catalog.md) | `src/backend/commands/dbcommands.c`              |
| `DBState`                                     | [03](03_catalog_data_model_and_bootstrap.md) | `src/include/catalog/pg_control.h`               |
| `DeactivateCommitTs`                          | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c`         |
| `DECLARE_INDEX`                               | [03](03_catalog_data_model_and_bootstrap.md) | catalog headers                                   |
| `DECLARE_UNIQUE_INDEX`                        | [03](03_catalog_data_model_and_bootstrap.md) | catalog headers                                   |
| `DECLARE_UNIQUE_INDEX_PKEY`                   | [03](03_catalog_data_model_and_bootstrap.md) | catalog headers                                   |
| `deleteOneObject`                             | [04](04_catalog_modification_apis.md) | `src/backend/catalog/dependency.c`                |
| `DEPENDENCY_AUTO`                             | [04](04_catalog_modification_apis.md) | `src/include/catalog/dependency.h`                |
| `DEPENDENCY_INTERNAL`                         | [04](04_catalog_modification_apis.md) | `src/include/catalog/dependency.h`                |
| `DEPENDENCY_NORMAL`                           | [04](04_catalog_modification_apis.md) | `src/include/catalog/dependency.h`                |

## E – F

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `ExecGrantStmt_oids`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/aclchk.c`                   |
| `ExtendCLOG`                                  | [09](09_clog.md) | `src/backend/access/transam/clog.c:959`          |
| `ExtendCommitTs`                              | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c`         |
| `findDependentObjects`                        | [04](04_catalog_modification_apis.md) | `src/backend/catalog/dependency.c`                |
| `FormData_pg_attribute`                       | [18](18_catalog_inventory.md) | `src/include/catalog/pg_attribute.h`              |
| `FormData_pg_class`                           | [18](18_catalog_inventory.md) | `src/include/catalog/pg_class.h`                  |
| `FormData_pg_proc`                            | [18](18_catalog_inventory.md) | `src/include/catalog/pg_proc.h`                   |
| `FormData_pg_type`                            | [18](18_catalog_inventory.md) | `src/include/catalog/pg_type.h`                   |
| `formrdesc`                                   | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c:1875`         |
| `FreeSpaceMapPrepareTruncateRel`              | [14](14_free_space_map.md), [20](20_wal_record_catalog.md) | `src/backend/storage/freespace/freespace.c`       |
| `FreeSpaceMapVacuum`                          | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:358`   |
| `FreeSpaceMapVacuumRange`                     | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:377`   |
| `fsm_does_block_exist`                        | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c`       |
| `fsm_get_location`                            | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c`       |
| `fsm_logical_to_physical`                     | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c`       |
| `fsm_search`                                  | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:678`   |
| `fsm_search_avail`                            | [14](14_free_space_map.md) | `src/backend/storage/freespace/fsmpage.c:158`     |
| `fsm_set_and_search`                          | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:646`   |
| `fsm_set_avail`                               | [14](14_free_space_map.md) | `src/backend/storage/freespace/fsmpage.c:63`      |
| `fsm_space_avail_to_cat`                      | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c`       |
| `fsm_vacuum_page`                             | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:812`   |

## G

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `genbki.pl`                                   | [03](03_catalog_data_model_and_bootstrap.md) | `src/backend/catalog/genbki.pl`                  |
| `get_object_address`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/objectaddress.c`            |
| `GetMultiXactIdMembers`                       | [12](12_multixact.md) | `src/backend/access/transam/multixact.c:1293`     |
| `GetNewMultiXactId`                           | [12](12_multixact.md) | `src/backend/access/transam/multixact.c:1026`     |
| `GetNewObjectId`                              | [20](20_wal_record_catalog.md) | `src/backend/access/transam/varsup.c`            |
| `GetNewTransactionId`                         | [09](09_clog.md), [11](11_commit_timestamps.md) | `src/backend/access/transam/varsup.c`            |
| `GetPageWithFreeSpace`                        | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:137`   |
| `GetVisibilityMapPins`                        | [13](13_visibility_map.md), [14](14_free_space_map.md) | `src/backend/access/heap/hio.c:140`               |
| `group_lsn`                                   | [08](08_slru_framework.md), [09](09_clog.md) | `src/include/access/slru.h`                       |

## H

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `HandleCatchupInterrupt`                      | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`                |
| `HEAPBLOCKS_PER_BYTE`                         | [13](13_visibility_map.md) | `src/include/access/visibilitymapdefs.h`          |
| `HEAPBLOCKS_PER_PAGE`                         | [13](13_visibility_map.md) | `src/include/access/visibilitymapdefs.h`          |
| `heap_create`                                 | [04](04_catalog_modification_apis.md) | `src/backend/catalog/heap.c`                     |
| `heap_create_with_catalog`                    | [04](04_catalog_modification_apis.md) | `src/backend/catalog/heap.c:1105`                |
| `heap_drop_with_catalog`                      | [04](04_catalog_modification_apis.md) | `src/backend/catalog/heap.c:1767`                |
| `heap_inplace_update_and_unlock`              | [04](04_catalog_modification_apis.md) | `src/backend/access/heap/heapam.c`               |
| `heap_xlog_visible`                           | [13](13_visibility_map.md), [20](20_wal_record_catalog.md) | `src/backend/access/heap/heapam.c`                |

## I

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `index_create`                                | [04](04_catalog_modification_apis.md) | `src/backend/catalog/index.c:724`                |
| `index_drop`                                  | [04](04_catalog_modification_apis.md) | `src/backend/catalog/index.c:2114`               |
| `IndexFreeSpaceMapVacuum`                     | [14](14_free_space_map.md) | `src/backend/storage/freespace/indexfsm.c`        |
| `InitCatCache`                                | [05](05_catalog_caches.md) | `src/backend/utils/cache/catcache.c`              |
| `InsertPgClassTuple`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/heap.c`                     |
| `InvalidateSystemCachesExtended`              | [06](06_cache_invalidation.md), [21](21_deep_dives.md) | `src/backend/utils/cache/inval.c`                |
| `InvokeObjectPostCreateHook`                  | [17](17_hooks_and_extensibility.md) | `src/include/catalog/objectaccess.h`              |
| `IsInplaceUpdateRelation`                     | [04](04_catalog_modification_apis.md) | `src/backend/catalog/heap.c`                     |
| `IsSharedRelation`                            | [03](03_catalog_data_model_and_bootstrap.md) | `src/backend/catalog/catalog.c`                   |

## L – M

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `latest_page_number`                          | [08](08_slru_framework.md) | `src/include/access/slru.h`                       |
| `load_critical_index`                         | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c`              |
| `load_relcache_init_file`                     | [05](05_catalog_caches.md), [21](21_deep_dives.md) | `src/backend/utils/cache/relcache.c`              |
| `load_relmap_file`                            | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c:765`         |
| `LocalExecuteInvalidationMessage`             | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`                  |
| `log_smgrcreate`                              | [04](04_catalog_modification_apis.md), [20](20_wal_record_catalog.md) | `src/backend/catalog/storage.c:186`                |
| `LookupExplicitNamespace`                     | [04](04_catalog_modification_apis.md) | `src/backend/catalog/namespace.c:3385`              |
| `MarkBufferDirtyHint`                         | [13](13_visibility_map.md), [14](14_free_space_map.md), [21](21_deep_dives.md) | `src/backend/storage/buffer/bufmgr.c`             |
| `MAX_MAPPINGS`                                | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`              |
| `MAX_RELCACHE_CALLBACKS`                      | [17](17_hooks_and_extensibility.md) | `src/backend/utils/cache/inval.c`                  |
| `MAX_SYSCACHE_CALLBACKS`                      | [17](17_hooks_and_extensibility.md) | `src/backend/utils/cache/inval.c`                  |
| `MaxFSMRequestSize`                           | [14](14_free_space_map.md) | `src/include/storage/fsm_internals.h`              |
| `MaxHeapTupleSize`                            | [14](14_free_space_map.md) | `src/include/access/htup.h`                        |
| `MULTIXACT_MEMBERS_PER_PAGE`                  | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |
| `MULTIXACT_OFFSETS_PER_PAGE`                  | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactAdvanceOldest`                      | [12](12_multixact.md) | `src/backend/access/transam/multixact.c:2528`      |
| `MultiXactGetCheckptMulti`                    | [12](12_multixact.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactGenLock`                            | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactIdCreate`                           | [12](12_multixact.md) | `src/backend/access/transam/multixact.c:433`       |
| `MultiXactIdCreateFromMembers`                | [12](12_multixact.md) | `src/backend/access/transam/multixact.c:814`       |
| `MultiXactIdExpand`                           | [12](12_multixact.md) | `src/backend/access/transam/multixact.c:486`       |
| `MultiXactIdGetUpdateXid`                     | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactIdIsRunning`                        | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactMember`                             | [12](12_multixact.md) | `src/include/access/multixact.h`                   |
| `MultiXactMemberCtl`                          | [12](12_multixact.md), [19](19_slru_users_catalog.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactMemberFreezeThreshold`              | [12](12_multixact.md), [21](21_deep_dives.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactOffsetCtl`                          | [12](12_multixact.md), [19](19_slru_users_catalog.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactSetNextMXact`                       | [12](12_multixact.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/multixact.c`           |
| `MultiXactStatus`                             | [12](12_multixact.md) | `src/include/access/multixact.h`                   |
| `multixact_redo`                              | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/backend/access/transam/multixact.c:3386`      |
| `multixact_twophase_postabort`                | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |
| `multixact_twophase_postcommit`               | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |
| `multixact_twophase_recover`                  | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`           |

## N – O

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `NewHeapCreateToastTable`                     | [04](04_catalog_modification_apis.md) | `src/backend/catalog/toasting.c`                  |
| `NotifyCtl`                                   | [19](19_slru_users_catalog.md) | `src/backend/commands/async.c`                    |
| `object_access_hook`                          | [17](17_hooks_and_extensibility.md) | `src/include/catalog/objectaccess.h`              |
| `ObjectProperty[]`                            | [04](04_catalog_modification_apis.md) | `src/backend/catalog/objectaddress.c`             |

## P

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `PagePrecedes` (callback)                     | [08](08_slru_framework.md) | `src/include/access/slru.h`                       |
| `PartitionDesc`                               | [05](05_catalog_caches.md), [18](18_catalog_inventory.md) | `src/include/partitioning/partdesc.h`              |
| `PartitionKey`                                | [05](05_catalog_caches.md), [18](18_catalog_inventory.md) | `src/include/partitioning/partbounds.h`             |
| `pendingDeletes`                              | [04](04_catalog_modification_apis.md), [20](20_wal_record_catalog.md) | `src/backend/catalog/storage.c`                   |
| `perform_relmap_update`                       | [07](07_relmapper.md), [20](20_wal_record_catalog.md) | `src/backend/utils/cache/relmapper.c`              |
| `performDeletion`                             | [04](04_catalog_modification_apis.md) | `src/backend/catalog/dependency.c:273`             |
| `PG_CONTROL_FILE_SIZE`                        | [03](03_catalog_data_model_and_bootstrap.md) | `src/include/catalog/pg_control.h:250`             |
| `PG_CONTROL_MAX_SAFE_SIZE`                    | [03](03_catalog_data_model_and_bootstrap.md) | `src/include/catalog/pg_control.h:241`             |
| `PG_CONTROL_VERSION`                          | [03](03_catalog_data_model_and_bootstrap.md) | `src/include/catalog/pg_control.h:25`              |
| `pg_internal.init`                            | [05](05_catalog_caches.md), [21](21_deep_dives.md) | `src/backend/utils/cache/relcache.c`               |
| `PG_RMGR`                                     | [15](15_persistence_and_wal_records.md), [21](21_deep_dives.md) | `src/include/access/rmgrlist.h`                    |
| `PrepareInvalidationState`                    | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`                  |
| `ProcedureCreate`                             | [04](04_catalog_modification_apis.md), [18](18_catalog_inventory.md) | `src/backend/catalog/pg_proc.c`                    |
| `ProcessCatchupInterrupt`                     | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`                  |
| `ProcessCommittedInvalidationMessages`        | [06](06_cache_invalidation.md), [15](15_persistence_and_wal_records.md), [21](21_deep_dives.md) | `src/backend/utils/cache/inval.c:962`              |
| `ProcessSyncRequests`                         | [16](16_checkpoints_and_recovery.md) | `src/backend/storage/sync/sync.c`                  |
| `ProcArrayEndTransaction`                     | [06](06_cache_invalidation.md), [15](15_persistence_and_wal_records.md) | `src/backend/storage/ipc/procarray.c`               |

## R

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `RangeVarGetRelid`                            | [04](04_catalog_modification_apis.md) | `src/include/catalog/namespace.h:80`               |
| `RangeVarGetRelidExtended`                    | [04](04_catalog_modification_apis.md) | `src/backend/catalog/namespace.c:441`              |
| `ReadControlFile`                             | [03](03_catalog_data_model_and_bootstrap.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/xlog.c:4298`           |
| `ReceiveSharedInvalidMessages`                | [06](06_cache_invalidation.md) | `src/backend/storage/ipc/sinval.c:70`              |
| `RecordAndGetPageWithFreeSpace`               | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:154`    |
| `RecordFreeIndexPage`                         | [14](14_free_space_map.md) | `src/backend/storage/freespace/indexfsm.c`         |
| `RecordNewMultiXact`                          | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/backend/access/transam/multixact.c`           |
| `RecordPageWithFreeSpace`                     | [14](14_free_space_map.md) | `src/backend/storage/freespace/freespace.c:194`    |
| `RecordTransactionAbort`                      | [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/xact.c:1723`            |
| `RecordTransactionCommit`                     | [06](06_cache_invalidation.md), [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/xact.c:1304`            |
| `RecordUsedIndexPage`                         | [14](14_free_space_map.md) | `src/backend/storage/freespace/indexfsm.c`         |
| `recordDependencyOn`                          | [04](04_catalog_modification_apis.md) | `src/backend/catalog/pg_depend.c:46`                |
| `recordSharedDependencyOn`                    | [03](03_catalog_data_model_and_bootstrap.md) | `src/backend/catalog/pg_shdepend.c`                 |
| `RegisterCatcacheInvalidation`                | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c:545`               |
| `RegisterCustomRmgr`                          | [17](17_hooks_and_extensibility.md), [21](21_deep_dives.md) | `src/backend/access/transam/xlog.c`                  |
| `RegisterSyncRequest`                         | [08](08_slru_framework.md) | `src/backend/storage/sync/sync.c`                   |
| `RelationBuildDesc`                           | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c:1040`           |
| `RelationCacheInitFileInvalidate`             | [05](05_catalog_caches.md), [21](21_deep_dives.md) | `src/backend/utils/cache/relcache.c`                |
| `RelationCacheInitialize`                     | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c`                |
| `RelationCacheInitializePhase2`               | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c`                |
| `RelationCacheInitializePhase3`               | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c:4102`           |
| `RelationClearRelation`                       | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c`                |
| `RelationClose`                               | [04](04_catalog_modification_apis.md), [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c:2194`            |
| `RelationCreateStorage`                       | [04](04_catalog_modification_apis.md) | `src/backend/catalog/storage.c:121`                  |
| `RelationData`                                | [05](05_catalog_caches.md) | `src/include/utils/rel.h`                            |
| `RelationDropStorage`                         | [04](04_catalog_modification_apis.md) | `src/backend/catalog/storage.c:206`                  |
| `RelationGetBufferForTuple`                   | [13](13_visibility_map.md), [14](14_free_space_map.md) | `src/backend/access/heap/hio.c:502`                  |
| `RelationIdCache`                             | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c`                  |
| `RelationIdGetRelation`                       | [05](05_catalog_caches.md) | `src/backend/utils/cache/relcache.c:2063`             |
| `RelationInitPhysicalAddr`                    | [05](05_catalog_caches.md), [07](07_relmapper.md) | `src/backend/utils/cache/relcache.c`                  |
| `RelationMapFinishBootstrap`                  | [03](03_catalog_data_model_and_bootstrap.md), [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`                 |
| `RelationMapInitialize`                       | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`                |
| `RelationMapInvalidate`                       | [06](06_cache_invalidation.md), [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`                |
| `RelationMapOidToFilenumber`                  | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c:165`             |
| `RelationMapUpdateMap`                        | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c:325`             |
| `RelationSetNewRelfilenumber`                 | [05](05_catalog_caches.md), [07](07_relmapper.md) | `src/backend/utils/cache/relcache.c`                  |
| `RelationTruncate`                            | [04](04_catalog_modification_apis.md), [13](13_visibility_map.md), [14](14_free_space_map.md) | `src/backend/catalog/storage.c`                      |
| `relmap_redo`                                 | [07](07_relmapper.md), [20](20_wal_record_catalog.md) | `src/backend/utils/cache/relmapper.c:1096`            |
| `RelMapFile`                                  | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`                 |
| `RelMapping`                                  | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`                 |
| `RELMAPPER_FILEMAGIC`                         | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c`                 |
| `ReleaseSysCache`                             | [05](05_catalog_caches.md) | `src/backend/utils/cache/syscache.c:269`              |

## S

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `SearchCatCacheInternal`                      | [05](05_catalog_caches.md) | `src/backend/utils/cache/catcache.c:1363`           |
| `SearchCatCacheList`                          | [05](05_catalog_caches.md) | `src/backend/utils/cache/catcache.c`                |
| `SearchCatCacheMiss`                          | [05](05_catalog_caches.md) | `src/backend/utils/cache/catcache.c`                |
| `SearchSysCache1`                             | [05](05_catalog_caches.md) | `src/backend/utils/cache/syscache.c:221`            |
| `SearchSysCache2`                             | [05](05_catalog_caches.md) | `src/backend/utils/cache/syscache.c`                 |
| `SearchSysCacheCopy1`                         | [05](05_catalog_caches.md) | `src/backend/utils/cache/syscache.c`                 |
| `SearchSysCacheLocked1`                       | [05](05_catalog_caches.md) | `src/backend/utils/cache/syscache.c`                 |
| `SendSharedInvalidMessages`                   | [06](06_cache_invalidation.md) | `src/backend/storage/ipc/sinval.c:48`              |
| `SerialSlruCtl`                               | [19](19_slru_users_catalog.md) | `src/backend/storage/lmgr/predicate.c`             |
| `SetMultiXactIdLimit`                         | [12](12_multixact.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/multixact.c`           |
| `SetOffsetVacuumLimit`                        | [12](12_multixact.md), [21](21_deep_dives.md) | `src/backend/access/transam/multixact.c:2705`       |
| `SetTransactionIdLimit`                       | [09](09_clog.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/varsup.c`               |
| `SharedInvalidationMessage`                   | [06](06_cache_invalidation.md) | `src/include/storage/sinval.h`                       |
| `SharedInvalidStateData`                      | [06](06_cache_invalidation.md) | `src/include/storage/sinvaladt.h`                    |
| `SICleanupQueue`                              | [06](06_cache_invalidation.md) | `src/backend/storage/ipc/sinvaladt.c`                |
| `SIInsertDataEntries`                         | [06](06_cache_invalidation.md) | `src/backend/storage/ipc/sinvaladt.c:370`            |
| `SIGetDataEntries`                            | [06](06_cache_invalidation.md) | `src/backend/storage/ipc/sinvaladt.c:473`            |
| `SimpleLruDoesPhysicalPageExist`              | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SimpleLruGetBankLock`                        | [08](08_slru_framework.md) | `src/include/access/slru.h`                          |
| `SimpleLruInit`                               | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SimpleLruReadPage`                           | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c:502`              |
| `SimpleLruReadPage_ReadOnly`                  | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SimpleLruTruncate`                           | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c:1405`             |
| `SimpleLruWaitIO`                             | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SimpleLruWriteAll`                           | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c:1319`             |
| `SimpleLruWritePage`                          | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c:729`              |
| `SimpleLruZeroPage`                           | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SInvalReadLock`                              | [06](06_cache_invalidation.md) | `src/include/storage/sinvaladt.h`                    |
| `SInvalWriteLock`                             | [06](06_cache_invalidation.md) | `src/include/storage/sinvaladt.h`                    |
| `SlruCtlData`                                 | [08](08_slru_framework.md) | `src/include/access/slru.h:127`                      |
| `SlruScanDirectory`                           | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SlruScanDirCbDeleteAll`                      | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SlruScanDirCbReportPresence`                 | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SlruSelectLRUPage`                           | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c:1166`             |
| `SlruSharedData`                              | [08](08_slru_framework.md) | `src/include/access/slru.h:61`                       |
| `SlruSyncFileTag`                             | [08](08_slru_framework.md) | `src/backend/access/transam/slru.c`                  |
| `SLRU_PAGE_*` (enum)                          | [08](08_slru_framework.md) | `src/include/access/slru.h`                           |
| `SLRU_PAGES_PER_SEGMENT`                      | [08](08_slru_framework.md) | `src/include/access/slru.h`                           |
| `smgrcreate`                                  | [04](04_catalog_modification_apis.md), [20](20_wal_record_catalog.md) | `src/backend/storage/smgr/smgr.c`                    |
| `smgrDoPendingDeletes`                        | [04](04_catalog_modification_apis.md), [15](15_persistence_and_wal_records.md) | `src/backend/catalog/storage.c`                      |
| `smgr_redo`                                   | [20](20_wal_record_catalog.md) | `src/backend/catalog/storage.c`                      |
| `SUBTRANS_XACTS_PER_PAGE`                     | [10](10_subtrans.md) | `src/backend/access/transam/subtrans.c`               |
| `SubTransCtl`                                 | [10](10_subtrans.md), [19](19_slru_users_catalog.md) | `src/backend/access/transam/subtrans.c`               |
| `SubTransGetParent`                           | [10](10_subtrans.md) | `src/backend/access/transam/subtrans.c`               |
| `SubTransGetTopmostTransaction`               | [10](10_subtrans.md), [21](21_deep_dives.md) | `src/backend/access/transam/subtrans.c:163`           |
| `SubTransSetParent`                           | [10](10_subtrans.md) | `src/backend/access/transam/subtrans.c:85`            |
| `SyncRequestHandler`                          | [08](08_slru_framework.md) | `src/include/storage/sync.h`                          |
| `SysCacheGetAttr`                             | [05](05_catalog_caches.md) | `src/backend/utils/cache/syscache.c`                  |
| `SysCacheGetAttrNotNull`                      | [05](05_catalog_caches.md) | `src/backend/utils/cache/syscache.c`                  |
| `SysCacheIdentifier`                          | [05](05_catalog_caches.md) | generated `syscache_info.h`                           |
| `SysCacheInvalidate`                          | [05](05_catalog_caches.md), [06](06_cache_invalidation.md) | `src/backend/utils/cache/syscache.c`                  |

## T

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `tblspc_redo`                                 | [20](20_wal_record_catalog.md) | `src/backend/commands/tablespace.c`                |
| `track_commit_timestamp`                      | [11](11_commit_timestamps.md), [appendix_guc_parameters.md](appendix_guc_parameters.md) | GUC                                                |
| `TransactionIdAbortTree`                      | [09](09_clog.md), [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/transam.c`              |
| `TransactionIdCommitTree`                     | [09](09_clog.md), [15](15_persistence_and_wal_records.md), [21](21_deep_dives.md) | `src/backend/access/transam/transam.c`              |
| `TransactionIdGetCommitTsData`                | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c:274`         |
| `TransactionIdGetStatus`                      | [09](09_clog.md) | `src/backend/access/transam/clog.c:735`              |
| `TransactionIdSetCommitTs`                    | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c:249`          |
| `TransactionIdSetTreeStatus`                  | [09](09_clog.md) | `src/backend/access/transam/clog.c:183`               |
| `TransactionIdSetPageStatus`                  | [09](09_clog.md) | `src/backend/access/transam/clog.c`                   |
| `TransactionIdSetPageStatusInternal`          | [09](09_clog.md) | `src/backend/access/transam/clog.c`                   |
| `TransactionIdSetStatusBit`                   | [09](09_clog.md) | `src/backend/access/transam/clog.c`                   |
| `TransactionGroupUpdateXidStatus`             | [09](09_clog.md), [21](21_deep_dives.md) | `src/backend/access/transam/clog.c:441`               |
| `TransactionTreeSetCommitTsData`              | [11](11_commit_timestamps.md), [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/commit_ts.c`              |
| `TransInvalidationInfo`                       | [06](06_cache_invalidation.md) | `src/backend/utils/cache/inval.c`                    |
| `TrimCLOG`                                    | [09](09_clog.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/clog.c:892`              |
| `TrimMultiXact`                               | [12](12_multixact.md), [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/multixact.c:2170`         |
| `TruncateCLOG`                                | [09](09_clog.md) | `src/backend/access/transam/clog.c:1000`              |
| `TruncateCommitTs`                            | [11](11_commit_timestamps.md) | `src/backend/access/transam/commit_ts.c`              |
| `TruncateMultiXact`                           | [12](12_multixact.md) | `src/backend/access/transam/multixact.c`              |
| `TruncateSUBTRANS`                            | [10](10_subtrans.md) | `src/backend/access/transam/subtrans.c`               |
| `TypeCreate`                                  | [03](03_catalog_data_model_and_bootstrap.md) | `src/backend/catalog/pg_type.c`                       |

## U – W

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `UpdateControlFile`                           | [16](16_checkpoints_and_recovery.md) | `src/backend/access/transam/xlog.c:4514`             |
| `UpdateIndexRelation`                         | [04](04_catalog_modification_apis.md) | `src/backend/catalog/index.c`                        |
| `vac_truncate_clog`                           | [09](09_clog.md), [11](11_commit_timestamps.md), [12](12_multixact.md) | `src/backend/commands/vacuum.c`                       |
| `VISIBILITYMAP_ALL_FROZEN`                    | [13](13_visibility_map.md) | `src/include/access/visibilitymapdefs.h`              |
| `VISIBILITYMAP_ALL_VISIBLE`                   | [13](13_visibility_map.md) | `src/include/access/visibilitymapdefs.h`              |
| `visibilitymap_clear`                         | [13](13_visibility_map.md) | `src/backend/access/heap/visibilitymap.c:138`         |
| `visibilitymap_count`                         | [13](13_visibility_map.md) | `src/backend/access/heap/visibilitymap.c:384`         |
| `visibilitymap_get_status`                    | [13](13_visibility_map.md) | `src/backend/access/heap/visibilitymap.c:336`         |
| `visibilitymap_pin`                           | [13](13_visibility_map.md), [21](21_deep_dives.md) | `src/backend/access/heap/visibilitymap.c:191`         |
| `visibilitymap_pin_ok`                        | [13](13_visibility_map.md) | `src/backend/access/heap/visibilitymap.c:215`         |
| `visibilitymap_prepare_truncate`              | [13](13_visibility_map.md), [20](20_wal_record_catalog.md) | `src/backend/access/heap/visibilitymap.c`             |
| `visibilitymap_set`                           | [13](13_visibility_map.md), [21](21_deep_dives.md) | `src/backend/access/heap/visibilitymap.c:244`         |
| `vm_extend`                                   | [13](13_visibility_map.md) | `src/backend/access/heap/visibilitymap.c:612`         |
| `vm_readbuf`                                  | [13](13_visibility_map.md) | `src/backend/access/heap/visibilitymap.c:538`         |
| `WriteControlFile`                            | [03](03_catalog_data_model_and_bootstrap.md) | `src/backend/access/transam/xlog.c`                   |
| `write_relcache_init_file`                    | [05](05_catalog_caches.md), [21](21_deep_dives.md) | `src/backend/utils/cache/relcache.c:6491`              |
| `write_relmap_file`                           | [07](07_relmapper.md) | `src/backend/utils/cache/relmapper.c:889`              |

## X

| Symbol                                        | Where     | Source                                           |
|-----------------------------------------------|-----------|--------------------------------------------------|
| `xact_redo`                                   | [15](15_persistence_and_wal_records.md), [20](20_wal_record_catalog.md) | `src/backend/access/transam/xact.c`                  |
| `xact_redo_abort`                             | [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/xact.c:6222`             |
| `xact_redo_commit`                            | [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/xact.c:6068`             |
| `XactCtl`                                     | [09](09_clog.md), [19](19_slru_users_catalog.md) | `src/backend/access/transam/clog.c`                  |
| `xactGetCommittedInvalidationMessages`        | [06](06_cache_invalidation.md), [15](15_persistence_and_wal_records.md) | `src/backend/utils/cache/inval.c:883`                |
| `XidStatus` enum                              | [09](09_clog.md) | `src/include/access/clog.h`                          |
| `xl_clog_truncate`                            | [09](09_clog.md), [20](20_wal_record_catalog.md) | `src/include/access/clog.h`                          |
| `xl_commit_ts_set`                            | [11](11_commit_timestamps.md), [20](20_wal_record_catalog.md) | `src/backend/access/transam/commit_ts.c`             |
| `xl_commit_ts_truncate`                       | [11](11_commit_timestamps.md), [20](20_wal_record_catalog.md) | `src/include/access/commit_ts.h`                     |
| `xl_dbase_create_file_copy_rec`               | [20](20_wal_record_catalog.md) | `src/include/commands/dbcommands_xlog.h`             |
| `xl_dbase_create_wal_log_rec`                 | [20](20_wal_record_catalog.md) | `src/include/commands/dbcommands_xlog.h`             |
| `xl_dbase_drop_rec`                           | [20](20_wal_record_catalog.md) | `src/include/commands/dbcommands_xlog.h`             |
| `xl_heap_visible`                             | [13](13_visibility_map.md), [20](20_wal_record_catalog.md) | `src/include/access/heapam_xlog.h:62`                |
| `xl_multixact_create`                         | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/include/access/multixact.h`                     |
| `xl_multixact_truncate`                       | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/include/access/multixact.h`                     |
| `xl_relmap_update`                            | [07](07_relmapper.md), [20](20_wal_record_catalog.md) | `src/include/utils/relmapper.h:27`                   |
| `xl_smgr_create`                              | [04](04_catalog_modification_apis.md), [20](20_wal_record_catalog.md) | `src/include/catalog/storage_xlog.h:30`              |
| `xl_smgr_truncate`                            | [04](04_catalog_modification_apis.md), [20](20_wal_record_catalog.md) | `src/include/catalog/storage_xlog.h:31`              |
| `xl_tblspc_create_rec`                        | [20](20_wal_record_catalog.md) | `src/include/commands/tablespace.h:25`               |
| `xl_tblspc_drop_rec`                          | [20](20_wal_record_catalog.md) | `src/include/commands/tablespace.h:26`               |
| `xl_xact_commit`                              | [06](06_cache_invalidation.md), [15](15_persistence_and_wal_records.md) | `src/include/access/xact.h`                          |
| `XLOG_CHECKPOINT_ONLINE`                      | [20](20_wal_record_catalog.md) | `src/include/catalog/pg_control.h:69`                |
| `XLOG_CHECKPOINT_REDO`                        | [20](20_wal_record_catalog.md) | `src/include/catalog/pg_control.h:82`                |
| `XLOG_CHECKPOINT_SHUTDOWN`                    | [20](20_wal_record_catalog.md) | `src/include/catalog/pg_control.h:68`                |
| `XLOG_CLOG_TRUNCATE`                          | [09](09_clog.md), [20](20_wal_record_catalog.md) | `src/include/access/clog.h:56`                       |
| `XLOG_CLOG_ZEROPAGE`                          | [09](09_clog.md), [20](20_wal_record_catalog.md) | `src/include/access/clog.h:55`                       |
| `XLOG_COMMIT_TS_SETTS`                        | [11](11_commit_timestamps.md), [20](20_wal_record_catalog.md) | `src/include/access/commit_ts.h`                     |
| `XLOG_COMMIT_TS_TRUNCATE`                     | [11](11_commit_timestamps.md), [20](20_wal_record_catalog.md) | `src/include/access/commit_ts.h:47`                  |
| `XLOG_COMMIT_TS_ZEROPAGE`                     | [11](11_commit_timestamps.md), [20](20_wal_record_catalog.md) | `src/include/access/commit_ts.h:46`                  |
| `XLOG_DBASE_CREATE_FILE_COPY`                 | [20](20_wal_record_catalog.md) | `src/include/commands/dbcommands_xlog.h:21`          |
| `XLOG_DBASE_CREATE_WAL_LOG`                   | [20](20_wal_record_catalog.md) | `src/include/commands/dbcommands_xlog.h:22`          |
| `XLOG_DBASE_DROP`                             | [20](20_wal_record_catalog.md) | `src/include/commands/dbcommands_xlog.h:23`          |
| `XLOG_FPI`                                    | [20](20_wal_record_catalog.md) | `src/include/catalog/pg_control.h:79`                |
| `XLOG_FPI_FOR_HINT`                           | [13](13_visibility_map.md), [14](14_free_space_map.md), [20](20_wal_record_catalog.md), [21](21_deep_dives.md) | `src/include/catalog/pg_control.h:78`                |
| `XLOG_HEAP2_VISIBLE`                          | [13](13_visibility_map.md), [20](20_wal_record_catalog.md) | `src/include/access/heapam_xlog.h:62`                |
| `XLOG_MULTIXACT_CREATE_ID`                    | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/include/access/multixact.h:70`                  |
| `XLOG_MULTIXACT_TRUNCATE_ID`                  | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/include/access/multixact.h:71`                  |
| `XLOG_MULTIXACT_ZERO_MEM_PAGE`                | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/include/access/multixact.h:69`                  |
| `XLOG_MULTIXACT_ZERO_OFF_PAGE`                | [12](12_multixact.md), [20](20_wal_record_catalog.md) | `src/include/access/multixact.h:68`                  |
| `XLOG_NEXTOID`                                | [20](20_wal_record_catalog.md) | `src/include/catalog/pg_control.h:71`                |
| `XLOG_RELMAP_UPDATE`                          | [07](07_relmapper.md), [20](20_wal_record_catalog.md) | `src/include/utils/relmapper.h:25`                   |
| `XLOG_SMGR_CREATE`                            | [04](04_catalog_modification_apis.md), [20](20_wal_record_catalog.md) | `src/include/catalog/storage_xlog.h:30`              |
| `XLOG_SMGR_TRUNCATE`                          | [04](04_catalog_modification_apis.md), [20](20_wal_record_catalog.md) | `src/include/catalog/storage_xlog.h:31`              |
| `XLOG_TBLSPC_CREATE`                          | [20](20_wal_record_catalog.md) | `src/include/commands/tablespace.h:25`               |
| `XLOG_TBLSPC_DROP`                            | [20](20_wal_record_catalog.md) | `src/include/commands/tablespace.h:26`               |
| `XLOG_XACT_ABORT`                             | [15](15_persistence_and_wal_records.md) | `src/include/access/xact.h`                          |
| `XLOG_XACT_COMMIT`                            | [06](06_cache_invalidation.md), [15](15_persistence_and_wal_records.md) | `src/include/access/xact.h`                          |
| `XLogBeginInsert`                             | [04](04_catalog_modification_apis.md) | `src/backend/access/transam/xloginsert.c`             |
| `XLogFlush`                                   | [09](09_clog.md), [13](13_visibility_map.md), [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/xlog.c`                  |
| `XLogInsert`                                  | [04](04_catalog_modification_apis.md), [15](15_persistence_and_wal_records.md) | `src/backend/access/transam/xloginsert.c`             |
| `XLogRecordPageWithFreeSpace`                 | [14](14_free_space_map.md), [21](21_deep_dives.md) | `src/backend/storage/freespace/freespace.c:211`       |
| `XLOG_FPI_FOR_HINT` (info-byte 0xA0)          | [13](13_visibility_map.md), [21](21_deep_dives.md) | `src/include/catalog/pg_control.h:78`                 |
| `xlog_redo`                                   | [20](20_wal_record_catalog.md) | `src/backend/access/transam/xlog.c`                   |

---

[Up: index.md](index.md)  |  [Prev: 21 Deep Dives](21_deep_dives.md)  |  [Next: appendix_glossary.md](appendix_glossary.md)
