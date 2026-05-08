# Metadata API Reference

[Up: index.md](index.md)

Function signatures grouped by subsystem, with one-line descriptions.
For detailed usage and call-graphs, follow the chapter links.

## Catalog modification (chapter [04](04_catalog_modification_apis.md))

```c
/* indexing.c — sanctioned mutators */
void CatalogTupleInsert (Relation heapRel, HeapTuple tup);
/* indexing.c:233 — heap_insert + CatalogIndexInsert + invalidation queue */

void CatalogTupleUpdate (Relation heapRel, ItemPointer otid, HeapTuple tup);
/* indexing.c:313 — heap_update + index reinsert */

void CatalogTupleDelete (Relation heapRel, ItemPointer tid);
/* indexing.c:365 — simple_heap_delete (vacuum cleans index pointers) */

void CatalogTupleInsertWithInfo(Relation, HeapTuple, CatalogIndexState);
void CatalogTuplesMultiInsertWithInfo(Relation, TupleTableSlot **, int, CatalogIndexState);

/* heap.c */
Oid  heap_create_with_catalog(const char *relname, Oid relnamespace, Oid reltablespace,
                              Oid relid, Oid reltypeid, Oid reloftypeid, Oid ownerid,
                              Oid accessmtd, TupleDesc tupdesc, List *cooked_constraints,
                              char relkind, char relpersistence,
                              bool shared_relation, bool mapped_relation,
                              OnCommitAction oncommit, Datum reloptions,
                              bool use_user_acl, bool allow_system_table_mods,
                              bool is_internal, Oid relrewrite,
                              ObjectAddress *typaddress);
/* heap.c:1105 — CREATE TABLE entry point */

void heap_drop_with_catalog(Oid relid);
/* heap.c:1767 — DROP entry point */

Relation heap_create(const char *relname, Oid namespaceid, Oid tablespace,
                     Oid relid, RelFileNumber relfilenumber, Oid accessmtd,
                     TupleDesc tupdesc, char relkind, char relpersistence,
                     bool shared_relation, bool mapped_relation,
                     bool allow_system_table_mods, TransactionId *relfrozenxid,
                     MultiXactId *relminmxid, bool create_storage);
/* heap.c — physical create + Relation alloc */

void AddNewAttributeTuples(Oid new_rel_oid, TupleDesc tupdesc, char relkind);
void StoreConstraints(Relation rel, List *cooked_constraints, bool is_internal);
void RemoveAttributeById(Oid relid, AttrNumber attnum);
void DeleteRelationTuple(Oid relid);

/* index.c */
Oid  index_create(Relation heapRelation, const char *indexRelationName,
                  Oid indexRelationId, Oid parentIndexRelid, Oid parentConstraintId,
                  RelFileNumber relFileNumber, IndexInfo *indexInfo,
                  List *indexColNames, Oid accessMethodId, Oid tableSpaceId,
                  Oid *collationIds, Oid *opclassIds, Datum *opclassOptions,
                  int16 *coloptions, NullableDatum *stattargets, Datum reloptions,
                  bits16 flags, bits16 constr_flags, bool allow_system_table_mods,
                  bool is_internal, Oid *constraintId);
/* index.c:724 */

void index_drop(Oid indexId, bool concurrent, bool concurrent_lock_mode);
/* index.c:2114 */

void index_constraint_create(Relation heapRelation, Oid indexRelationId,
                             Oid parentConstraintId, IndexInfo *indexInfo,
                             const char *constraintName, char constraintType,
                             bits16 constr_flags, bool allow_system_table_mods,
                             bool is_internal);
void index_update_stats(Relation rel, bool hasindex,
                        double reltuples);
void IndexSetParentIndex(Relation partitionIdx, Oid parentOid);

/* dependency.c, pg_depend.c */
void recordDependencyOn(const ObjectAddress *depender,
                        const ObjectAddress *referenced,
                        DependencyType behavior);
/* pg_depend.c:46 */

void recordMultipleDependencies(const ObjectAddress *depender,
                                const ObjectAddress *referenced,
                                int nreferenced, DependencyType behavior);

void deleteDependencyRecordsFor(Oid classId, Oid objectId, bool skipExtensionDeps);
void deleteSharedDependencyRecordsFor(Oid classId, Oid objectId);

void performDeletion(const ObjectAddress *object,
                     DropBehavior behavior, int flags);
/* dependency.c:273 */

void performMultipleDeletions(const ObjectAddresses *objects,
                              DropBehavior behavior, int flags);

void recordDependencyOnExpr(const ObjectAddress *depender, Node *expr,
                            List *rtable, DependencyType behavior);

/* namespace.c */
Oid  RangeVarGetRelid(const RangeVar *relation, LOCKMODE lockmode, bool missing_ok);
/* macro -> RangeVarGetRelidExtended at namespace.c:441 */

Oid  RangeVarGetCreationNamespace(const RangeVar *newRelation);
Oid  LookupExplicitNamespace(const char *nspname, bool missing_ok);
/* namespace.c:3385 */

void recomputeNamespacePath(void);

/* storage.c */
SMgrRelation RelationCreateStorage(RelFileLocator rlocator,
                                   char relpersistence, bool register_delete);
/* storage.c:121 */

void log_smgrcreate(const RelFileLocator *rlocator, ForkNumber forkNum);
/* storage.c:186 — emits XLOG_SMGR_CREATE */

void RelationDropStorage(Relation rel);
/* storage.c:206 — schedules unlink at commit */

void smgrDoPendingDeletes(bool isCommit);
void smgrDoPendingSyncs(bool isCommit, bool isParallelWorker);
void RelationTruncate(Relation rel, BlockNumber nblocks);

/* toasting.c */
void NewHeapCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode,
                             Oid OIDOldToast);

/* aclchk.c */
void ExecGrantStmt_oids(InternalGrant *istmt);
AclMode pg_class_aclmask(Oid table_oid, Oid roleid, AclMode mask, AclMaskHow how);
AclMode pg_namespace_aclmask(Oid nsp_oid, Oid roleid, AclMode mask, AclMaskHow how);

/* objectaddress.c */
ObjectAddress get_object_address(ObjectType objtype, Node *object,
                                 Relation *relp, LOCKMODE lockmode, bool missing_ok);
```

## Catalog cache (chapter [05](05_catalog_caches.md))

```c
/* catcache.c */
HeapTuple SearchCatCacheInternal(CatCache *cache, int nkeys,
                                 Datum v1, Datum v2, Datum v3, Datum v4);
/* catcache.c:1363 */
void CatCacheInvalidate(CatCache *cache, uint32 hashValue);
/* catcache.c:625 */

CatCache *InitCatCache(int id, Oid reloid, Oid indexoid, int nkeys,
                       const int *key, int nbuckets);
void      ResetCatalogCaches(void);

/* syscache.c */
HeapTuple SearchSysCache1(int cacheId, Datum key1);
/* syscache.c:221 */
HeapTuple SearchSysCache2(int cacheId, Datum key1, Datum key2);
HeapTuple SearchSysCache3(int cacheId, Datum key1, Datum key2, Datum key3);
HeapTuple SearchSysCache4(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4);

void     ReleaseSysCache(HeapTuple tuple);
/* syscache.c:269 */
HeapTuple SearchSysCacheCopy1(int cacheId, Datum key1);
HeapTuple SearchSysCacheLocked1(int cacheId, Datum key1);
/* syscache.c:287 */

bool      SearchSysCacheExists(int cacheId, int nkeys, ...);
Oid       GetSysCacheOid(int cacheId, AttrNumber oidcol, ...);
uint32    GetSysCacheHashValue(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4);
HeapTuple SearchSysCacheAttName(Oid relid, const char *attname);
HeapTuple SearchSysCacheAttNum (Oid relid, AttrNumber attnum);

Datum     SysCacheGetAttr        (int cacheId, HeapTuple tup, AttrNumber, bool *isNull);
Datum     SysCacheGetAttrNotNull (int cacheId, HeapTuple tup, AttrNumber);

bool      RelationInvalidatesSnapshotsOnly(Oid reloid);
bool      RelationHasSysCache       (Oid reloid);
bool      RelationSupportsSysCache  (Oid reloid);

void      SysCacheInvalidate(int cacheId, uint32 hashValue);

/* relcache.c */
Relation  RelationIdGetRelation(Oid relid);
/* relcache.c:2063 */
Relation  RelationBuildDesc    (Oid targetRelId, bool insertIt);
/* relcache.c:1040 */
void      RelationClose       (Relation relation);
/* relcache.c:2194 */

void RelationCacheInitialize         (void);
void RelationCacheInitializePhase2   (void);
void RelationCacheInitializePhase3   (void);
/* relcache.c:4102 */

void RelationCacheInvalidate     (bool debug_discard);
void RelationCacheInvalidateEntry(Oid relid);
void RelationFlushRelation       (Relation rel);
void RelationForgetRelation      (Oid relid);
void RelationClearRelation       (Relation relation, bool rebuild);

void RelationSetNewRelfilenumber(Relation relation, char persistence);

void formrdesc(const char *relationName, Oid relationReltype, bool isshared,
               int natts, const FormData_pg_attribute *attrs);
/* relcache.c:1875 */

bool write_relcache_init_file(bool shared);
/* relcache.c:6491 */
void RelationCacheInitFilePreInvalidate(void);
void RelationCacheInitFilePostInvalidate(void);
void RelationCacheInitFileRemove(void);

/* plancache.c, partcache.c, typcache.c, evtcache.c, ts_cache.c, attoptcache.c, spccache.c, relfilenumbermap.c */
/* (auxiliary caches; see chapter 05) */
```

## Cache invalidation (chapter [06](06_cache_invalidation.md))

```c
/* inval.c */
void CacheInvalidateHeapTuple(Relation, HeapTuple, HeapTuple newtup);
/* inval.c:1207 */
void CacheInvalidateHeapTupleByRelid(Oid relid, HeapTuple, HeapTuple newtup);
void CacheInvalidateRelcache       (Relation rel);
/* inval.c:1363 */
void CacheInvalidateRelcacheByTuple(HeapTuple classTuple);
void CacheInvalidateRelcacheByRelid(Oid relid);
void CacheInvalidateRelcacheAll    (void);
void CacheInvalidateCatalog        (Oid catalogId);
void CacheInvalidateSmgr           (RelFileLocatorBackend rlocator);
void CacheInvalidateRelmap         (Oid databaseId);

void RegisterCatcacheInvalidation  (int cacheId, uint32 hashValue, Oid dbId);

int  xactGetCommittedInvalidationMessages(SharedInvalidationMessage **msgs,
                                          bool *RelcacheInitFileInval);
/* inval.c:883 */
void ProcessCommittedInvalidationMessages(SharedInvalidationMessage *msgs,
                                          int nmsgs,
                                          bool RelcacheInitFileInval,
                                          Oid dbId, Oid tsId);
/* inval.c:962 */

void AcceptInvalidationMessages(void);
void AtEOXact_Inval            (bool isCommit);
/* inval.c:1026 */
void AtEOSubXact_Inval         (bool isCommit);
void CommandEndInvalidationMessages(void);
void LocalExecuteInvalidationMessage(SharedInvalidationMessage *msg);

void CacheRegisterSyscacheCallback(int cacheid, SyscacheCallbackFunction, Datum);
/* inval.c:1519 */
void CacheRegisterRelcacheCallback(RelcacheCallbackFunction, Datum);
/* inval.c:1561 */

/* sinval.c */
void SendSharedInvalidMessages   (const SharedInvalidationMessage *msgs, int n);
/* sinval.c:48 */
void ReceiveSharedInvalidMessages(void (*invalFunction) (SharedInvalidationMessage *),
                                  void (*resetFunction) (void));
/* sinval.c:70 */
void HandleCatchupInterrupt      (void);
void ProcessCatchupInterrupt     (void);

/* sinvaladt.c */
void SIInsertDataEntries(const SharedInvalidationMessage *data, int n);
/* sinvaladt.c:370 */
int  SIGetDataEntries   (SharedInvalidationMessage *data, int datasize);
/* sinvaladt.c:473 */
void SICleanupQueue     (bool callerHasWriteLock, int minFree);
```

## Relmapper (chapter [07](07_relmapper.md))

```c
RelFileNumber RelationMapOidToFilenumber           (Oid relationId, bool shared);
/* relmapper.c:165 */
Oid           RelationMapFilenumberToOid           (RelFileNumber, bool shared);
RelFileNumber RelationMapOidToFilenumberForDatabase(char *dbpath, Oid relationId);

void RelationMapUpdateMap (Oid relationId, RelFileNumber, bool shared, bool immediate);
/* relmapper.c:325 */
void AtCCI_RelationMap    (void);
void AtEOXact_RelationMap (bool isCommit, bool isParallelWorker);
void AtPrepare_RelationMap(void);

void perform_relmap_update(bool shared, const RelMapFile *updates);
void load_relmap_file     (bool shared, bool lock_held);
/* relmapper.c:765 */
void write_relmap_file    (RelMapFile *newmap, bool write_wal,
                           bool send_sinval, bool preserve_files,
                           Oid dbid, Oid tsid, const char *dbpath);
/* relmapper.c:889 */

void relmap_redo                  (XLogReaderState *record);
/* relmapper.c:1096 */
void CheckPointRelationMap        (void);
void RelationMapInvalidate        (bool shared);
void RelationMapInitialize        (void);
void RelationMapInitializePhase2  (void);
void RelationMapInitializePhase3  (void);
void RelationMapFinishBootstrap   (void);

Size EstimateRelationMapSpace(void);
void SerializeRelationMap   (Size maxSize, char *startAddress);
void RestoreRelationMap     (char *startAddress);
```

## SLRU framework (chapter [08](08_slru_framework.md))

```c
void SimpleLruInit(SlruCtl, const char *name, int nslots, int nlsns,
                   const char *subdir, int buffer_tranche_id, int bank_tranche_id,
                   SyncRequestHandler sync_handler, bool long_segment_names);

int  SimpleLruZeroPage          (SlruCtl, int64 pageno);
int  SimpleLruReadPage          (SlruCtl, int64 pageno, bool write_ok, TransactionId xid);
/* slru.c:502 */
int  SimpleLruReadPage_ReadOnly (SlruCtl, int64 pageno, TransactionId xid);
void SimpleLruWritePage         (SlruCtl, int slotno);
/* slru.c:729 */
void SimpleLruWriteAll          (SlruCtl, bool allow_redirtied);
/* slru.c:1319 */
void SimpleLruTruncate          (SlruCtl, int64 cutoffPage);
/* slru.c:1405 */
bool SimpleLruDoesPhysicalPageExist(SlruCtl, int64 pageno);

int  SlruScanDirectory      (SlruCtl, SlruScanCallback callback, void *data);
bool SlruScanDirCbDeleteAll (SlruCtl, char *filename, int64 segpage, void *data);
bool SlruScanDirCbReportPresence(SlruCtl, char *filename, int64 segpage, void *data);

LWLock *SimpleLruGetBankLock(SlruCtl, int64 pageno);   /* inline; slru.h:174 */

bool SlruSyncFileTag(SlruCtl, const FileTag *ftag, char *path);
```

## CLOG (chapter [09](09_clog.md))

```c
void      TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
                                     TransactionId *subxids,
                                     XidStatus status, XLogRecPtr lsn);
/* clog.c:183 */
XidStatus TransactionIdGetStatus    (TransactionId xid, XLogRecPtr *lsn);
/* clog.c:735 */
bool      TransactionGroupUpdateXidStatus(TransactionId, XidStatus, XLogRecPtr, int64);
/* clog.c:441 */

void BootStrapCLOG(void);
void StartupCLOG  (void);  /* clog.c:877 */
void TrimCLOG     (void);  /* clog.c:892 */
void CheckPointCLOG(void); /* clog.c:937 */
void ExtendCLOG   (TransactionId newestXact); /* clog.c:959 */
void TruncateCLOG (TransactionId oldestXact, Oid oldestxid_datoid); /* clog.c:1000 */
void clog_redo    (XLogReaderState *record); /* clog.c:1107 */

void AdvanceOldestClogXid(TransactionId oldestXid);
```

## SUBTRANS (chapter [10](10_subtrans.md))

```c
void          SubTransSetParent           (TransactionId xid, TransactionId parent);
/* subtrans.c:85 */
TransactionId SubTransGetParent           (TransactionId xid);
TransactionId SubTransGetTopmostTransaction(TransactionId xid);
/* subtrans.c:163 */

void StartupSUBTRANS    (TransactionId oldestActiveXID);
void CheckPointSUBTRANS (void);
void TruncateSUBTRANS   (TransactionId oldestXact);
void BootStrapSUBTRANS  (void);
```

## CommitTs (chapter [11](11_commit_timestamps.md))

```c
void TransactionIdSetCommitTs       (TransactionId xid, TimestampTz ts,
                                     RepOriginId nodeid, int slotno);
/* commit_ts.c:249 */
bool TransactionIdGetCommitTsData   (TransactionId xid, TimestampTz *ts,
                                     RepOriginId *nodeid);
/* commit_ts.c:274 */
void TransactionTreeSetCommitTsData(TransactionId xid, int nsubxids,
                                    TransactionId *subxids,
                                    TimestampTz timestamp, RepOriginId nodeid);

void GetLatestCommitTsData (TimestampTz *ts, RepOriginId *nodeid);

void ActivateCommitTs   (void);
void DeactivateCommitTs (void);

void StartupCommitTs    (void);
void CheckPointCommitTs (void);
void ExtendCommitTs     (TransactionId newestXact);
void TruncateCommitTs   (TransactionId oldestXact);

void commit_ts_redo     (XLogReaderState *record); /* commit_ts.c:1023 */
```

## MultiXact (chapter [12](12_multixact.md))

```c
MultiXactId MultiXactIdCreate            (TransactionId xid1, MultiXactStatus,
                                          TransactionId xid2, MultiXactStatus);
/* multixact.c:433 */
MultiXactId MultiXactIdExpand            (MultiXactId multi, TransactionId xid,
                                          MultiXactStatus status);
/* multixact.c:486 */
MultiXactId MultiXactIdCreateFromMembers (int nmembers, MultiXactMember *members);
/* multixact.c:814 */

int         GetMultiXactIdMembers       (MultiXactId, MultiXactMember **,
                                         bool from_pgupgrade, bool isLockOnly);
/* multixact.c:1293 */
bool        MultiXactIdIsRunning        (MultiXactId multi, bool isLockOnly);
TransactionId MultiXactIdGetUpdateXid   (MultiXactId multi);

void StartupMultiXact   (void); /* multixact.c:2145 */
void TrimMultiXact      (void); /* multixact.c:2170 */
void CheckPointMultiXact(void);
void TruncateMultiXact  (MultiXactId newOldestMulti, Oid newOldestMultiDB);

void MultiXactAdvanceOldest    (MultiXactId, Oid); /* multixact.c:2528 */
void SetOffsetVacuumLimit      (bool is_startup);  /* multixact.c:2705 */
void MultiXactSetNextMXact     (MultiXactId nextMulti, MultiXactOffset nextMultiOffset);
MultiXactOffset MultiXactMemberFreezeThreshold(void);

void multixact_redo            (XLogReaderState *record); /* multixact.c:3386 */

/* 2PC support */
void multixact_twophase_recover   (FullTransactionId, void *recdata, uint32 len);
void multixact_twophase_postcommit(FullTransactionId, void *recdata, uint32 len);
void multixact_twophase_postabort (FullTransactionId, void *recdata, uint32 len);
```

## Visibility Map (chapter [13](13_visibility_map.md))

```c
void   visibilitymap_set        (Relation rel, BlockNumber heapBlk,
                                 Buffer heapBuf, XLogRecPtr recptr,
                                 Buffer vmBuf, TransactionId cutoff_xid,
                                 uint8 flags);
/* visibilitymap.c:244 */
bool   visibilitymap_clear      (Relation rel, BlockNumber heapBlk,
                                 Buffer vmbuf, uint8 flags);
/* visibilitymap.c:138 */
uint8  visibilitymap_get_status (Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
/* visibilitymap.c:336 */
void   visibilitymap_pin        (Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
/* visibilitymap.c:191 */
bool   visibilitymap_pin_ok     (BlockNumber heapBlk, Buffer vmbuf);
/* visibilitymap.c:215 */
void   visibilitymap_count      (Relation rel, BlockNumber *all_visible,
                                 BlockNumber *all_frozen);
/* visibilitymap.c:384 */
BlockNumber visibilitymap_prepare_truncate(Relation rel, BlockNumber nheapblocks);

Buffer vm_readbuf(Relation rel, BlockNumber blkno, bool extend);
/* visibilitymap.c:538 */
void   vm_extend (Relation rel, BlockNumber vm_nblocks);
/* visibilitymap.c:612 */

void heap_xlog_visible(XLogReaderState *record);
```

## Free Space Map (chapter [14](14_free_space_map.md))

```c
BlockNumber GetPageWithFreeSpace          (Relation rel, Size spaceNeeded);
/* freespace.c:137 */
BlockNumber RecordAndGetPageWithFreeSpace (Relation rel, BlockNumber oldPage,
                                           Size oldSpaceAvail, Size spaceNeeded);
/* freespace.c:154 */
void        RecordPageWithFreeSpace       (Relation rel, BlockNumber heapBlk,
                                           Size spaceAvail);
/* freespace.c:194 */
void        XLogRecordPageWithFreeSpace   (RelFileLocator rlocator, BlockNumber heapBlk,
                                           Size spaceAvail);
/* freespace.c:211 */

void FreeSpaceMapVacuum     (Relation rel);                         /* freespace.c:358 */
void FreeSpaceMapVacuumRange(Relation rel, BlockNumber start, BlockNumber end);
/* freespace.c:377 */
void FreeSpaceMapPrepareTruncateRel(Relation rel, BlockNumber nblocks);

bool fsm_does_block_exist(Relation rel, BlockNumber blkno);

/* fsmpage.c — page-internal binary heap */
int  fsm_search_avail(Buffer buf, uint8 minvalue, bool advancenext, bool exclusive_lock_held);
/* fsmpage.c:158 */
void fsm_set_avail   (Page page, int slot, uint8 newvalue);
/* fsmpage.c:63 */
bool fsm_truncate_avail(Page page, int nslots);
bool fsm_rebuild_page (Page page);

/* hio.c — the bridge */
Buffer RelationGetBufferForTuple(Relation relation, Size len,
                                 Buffer otherBuffer, int options,
                                 BulkInsertState bistate, Buffer *vmbuffer,
                                 Buffer *vmbuffer_other, int num_pages);
/* hio.c:502 */
void   GetVisibilityMapPins(Relation relation, Buffer buffer1, Buffer buffer2,
                            BlockNumber block1, BlockNumber block2,
                            Buffer *vmbuffer1, Buffer *vmbuffer2);
/* hio.c:140 */

/* indexfsm.c */
BlockNumber GetFreeIndexPage      (Relation rel);
void        RecordFreeIndexPage   (Relation rel, BlockNumber blkno);
void        RecordUsedIndexPage   (Relation rel, BlockNumber blkno);
void        IndexFreeSpaceMapVacuum(Relation rel);
```

## Persistence and recovery (chapters [15](15_persistence_and_wal_records.md), [16](16_checkpoints_and_recovery.md))

```c
/* xact.c */
void RecordTransactionCommit(void);   /* xact.c:1304 */
void RecordTransactionAbort (bool isSubXact);  /* xact.c:1723 */
void xact_redo_commit       (xl_xact_parsed_commit *parsed,
                             TransactionId xid, XLogRecPtr lsn,
                             RepOriginId origin_id);
/* xact.c:6068 */
void xact_redo_abort        (xl_xact_parsed_abort *parsed,
                             TransactionId xid, XLogRecPtr lsn,
                             RepOriginId origin_id);
/* xact.c:6222 */

/* xlog.c */
void CreateCheckPoint   (int flags);              /* xlog.c:6863 */
void CheckPointGuts     (XLogRecPtr redo, int flags); /* xlog.c:7504 */
void UpdateControlFile  (void);                   /* xlog.c:4514 */
void ReadControlFile    (void);                   /* xlog.c:4298 */
void StartupXLOG        (void);                   /* xlog.c:5384 */
void CreateRestartPoint (int flags);

XLogRecPtr XLogInsert       (RmgrId rmid, uint8 info);
void       XLogFlush        (XLogRecPtr record);
XLogRecPtr XLogBeginInsert  (void);
void       XLogRegisterData (char *data, uint32 len);
void       XLogRegisterBuffer(uint8 block_id, Buffer buffer, uint8 flags);
```

## Hooks (chapter [17](17_hooks_and_extensibility.md))

```c
/* objectaccess.h — extension-installable hook */
typedef void (*object_access_hook_type)(ObjectAccessType, Oid classId, Oid objectId,
                                        int subId, void *arg);
extern PGDLLIMPORT object_access_hook_type object_access_hook;

#define InvokeObjectPostCreateHook(classId, objectId, subId)
#define InvokeObjectDropHook       (classId, objectId, subId)
#define InvokeObjectPostAlterHook  (classId, objectId, subId)
#define InvokeNamespaceSearchHook  (namespaceOid, ereport_on_violation)
#define InvokeFunctionExecuteHook  (functionOid)

/* Custom WAL rmgr */
void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr);

/* Catcache and relcache callback registries (also in chapter 06) */
void CacheRegisterSyscacheCallback(int cacheid, SyscacheCallbackFunction, Datum);
void CacheRegisterRelcacheCallback(RelcacheCallbackFunction, Datum);
```

---

[Up: index.md](index.md)
