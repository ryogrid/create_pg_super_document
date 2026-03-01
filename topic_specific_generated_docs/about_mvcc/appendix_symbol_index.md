# Appendix: Symbol Index

> MVCC Documentation > Appendix > Symbol Index

---

Alphabetical index of all 74 MVCC-related symbols documented in this documentation set. The "Documented In" column links to the chapter with the most detailed coverage.

| Symbol | Source File | Category | Importance | Documented In |
|--------|------------|----------|------------|---------------|
| `AbortTransaction` | `src/backend/access/transam/xact.c` | transaction | 0.90 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `AtEOXact_Snapshot` | `src/backend/utils/time/snapmgr.c` | snapshot | 0.70 | [Snapshot Management](06_snapshot_management.md) |
| `CheckForSerializableConflictIn` | `src/backend/storage/lmgr/predicate.c` | concurrency | 0.68 | [Deep Dives: SSI](10_deep_dives.md) |
| `CheckForSerializableConflictOut` | `src/backend/storage/lmgr/predicate.c` | concurrency | 0.66 | [Deep Dives: SSI](10_deep_dives.md) |
| `CommitTransaction` | `src/backend/access/transam/xact.c` | transaction | 0.95 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `CopySnapshot` | `src/backend/utils/time/snapmgr.c` | snapshot | 0.68 | [Snapshot Management](06_snapshot_management.md) |
| `CreateSharedProcArray` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.72 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `FreezeMultiXactId` | `src/backend/access/heap/heapam.c` | vacuum | 0.74 | [Deep Dives: MultiXact](10_deep_dives.md) |
| `GetCurrentCommandId` | `src/backend/access/transam/xact.c` | transaction | 0.72 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `GetCurrentTransactionId` | `src/backend/access/transam/xact.c` | transaction | 0.78 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `GetNewTransactionId` | `src/backend/access/transam/varsup.c` | transaction | 0.84 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `GetOldestActiveTransactionId` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.68 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `GetOldestNonRemovableTransactionId` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.80 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `GetSnapshotData` | `src/backend/storage/ipc/procarray.c` | snapshot | 0.96 | [Snapshot Management](06_snapshot_management.md) |
| `GetSnapshotDataReuse` | `src/backend/storage/ipc/procarray.c` | snapshot | 0.68 | [Snapshot Management](06_snapshot_management.md) |
| `GetTransactionSnapshot` | `src/backend/utils/time/snapmgr.c` | snapshot | 0.87 | [Snapshot Management](06_snapshot_management.md) |
| `GlobalVisTestIsRemovableXid` | `src/backend/storage/ipc/procarray.c` | vacuum | 0.68 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `HeapTupleFields` | `src/include/access/htup_details.h` | tuple | 0.80 | [Tuple Versioning](04_tuple_versioning.md) |
| `HeapTupleGetUpdateXid` | `src/backend/access/heap/heapam.c` | tuple | 0.72 | [Tuple Versioning](04_tuple_versioning.md) |
| `HeapTupleHeaderAdvanceConflictHorizon` | `src/backend/access/heap/heapam.c` | vacuum | 0.65 | [Deep Dives: MVCC+WAL](10_deep_dives.md) |
| `HeapTupleHeaderData` | `src/include/access/htup_details.h` | tuple | 0.97 | [Tuple Versioning](04_tuple_versioning.md) |
| `HeapTupleHeaderIsOnlyLocked` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.68 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesDirty` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.72 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesHistoricMVCC` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.60 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesMVCC` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.98 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesNonVacuumable` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.68 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesSelf` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.75 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesUpdate` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.88 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesVacuum` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.86 | [Visibility Rules](05_visibility_rules.md) |
| `HeapTupleSatisfiesVacuumHorizon` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.84 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |
| `HeapTupleSatisfiesVisibility` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.82 | [Visibility Rules](05_visibility_rules.md) |
| `PGPROC` | `src/include/storage/proc.h` | concurrency | 0.94 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `PROC_HDR` | `src/include/storage/proc.h` | concurrency | 0.75 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `PopActiveSnapshot` | `src/backend/utils/time/snapmgr.c` | snapshot | 0.78 | [Snapshot Management](06_snapshot_management.md) |
| `PreCommit_CheckForSerializationFailure` | `src/backend/storage/lmgr/predicate.c` | concurrency | 0.65 | [Deep Dives: SSI](10_deep_dives.md) |
| `ProcArrayAdd` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.70 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `ProcArrayEndTransaction` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.86 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `ProcArrayGroupClearXid` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.72 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `ProcArrayRemove` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.70 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `PushActiveSnapshot` | `src/backend/utils/time/snapmgr.c` | snapshot | 0.80 | [Snapshot Management](06_snapshot_management.md) |
| `RecordTransactionAbort` | `src/backend/access/transam/xact.c` | transaction | 0.78 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `RecordTransactionCommit` | `src/backend/access/transam/xact.c` | transaction | 0.89 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `RegisterSnapshot` | `src/backend/utils/time/snapmgr.c` | snapshot | 0.65 | [Snapshot Management](06_snapshot_management.md) |
| `SetHintBits` | `src/backend/access/heap/heapam_visibility.c` | visibility | 0.87 | [Visibility Rules](05_visibility_rules.md) |
| `SimpleLruInit` | `src/backend/access/transam/slru.c` | clog | 0.65 | [CLOG](08_clog_transaction_status.md) |
| `SnapshotData` | `src/include/utils/snapshot.h` | snapshot | 0.96 | [Snapshot Management](06_snapshot_management.md) |
| `SnapshotType` | `src/include/utils/snapshot.h` | snapshot | 0.72 | [Snapshot Management](06_snapshot_management.md) |
| `StartTransaction` | `src/backend/access/transam/xact.c` | transaction | 0.94 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `SubTransGetTopmostTransaction` | `src/backend/access/transam/subtrans.c` | transaction | 0.70 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `SubTransSetParent` | `src/backend/access/transam/subtrans.c` | transaction | 0.68 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `TransactionIdAbortTree` | `src/backend/access/transam/transam.c` | clog | 0.72 | [CLOG](08_clog_transaction_status.md) |
| `TransactionIdCommitTree` | `src/backend/access/transam/transam.c` | clog | 0.76 | [CLOG](08_clog_transaction_status.md) |
| `TransactionIdDidAbort` | `src/backend/access/transam/transam.c` | clog | 0.74 | [CLOG](08_clog_transaction_status.md) |
| `TransactionIdDidCommit` | `src/backend/access/transam/transam.c` | clog | 0.90 | [CLOG](08_clog_transaction_status.md) |
| `TransactionIdGetCommitLSN` | `src/backend/access/transam/transam.c` | clog | 0.70 | [CLOG](08_clog_transaction_status.md) |
| `TransactionIdGetStatus` | `src/backend/access/transam/clog.c` | clog | 0.78 | [CLOG](08_clog_transaction_status.md) |
| `TransactionIdIsCurrentTransactionId` | `src/backend/access/transam/xact.c` | transaction | 0.85 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `TransactionIdIsInProgress` | `src/backend/storage/ipc/procarray.c` | concurrency | 0.92 | [Concurrency Infrastructure](07_concurrency_infrastructure.md) |
| `TransactionIdSetPageStatus` | `src/backend/access/transam/clog.c` | clog | 0.76 | [CLOG](08_clog_transaction_status.md) |
| `TransactionIdSetTreeStatus` | `src/backend/access/transam/clog.c` | clog | 0.85 | [CLOG](08_clog_transaction_status.md) |
| `TransamVariablesData` | `src/include/access/transam.h` | transaction | 0.78 | [Transaction Lifecycle](03_transaction_lifecycle.md) |
| `XidInMVCCSnapshot` | `src/backend/utils/time/snapmgr.c` | visibility | 0.91 | [Visibility Rules](05_visibility_rules.md) |
| `heap_delete` | `src/backend/access/heap/heapam.c` | tuple | 0.92 | [Tuple Versioning](04_tuple_versioning.md) |
| `heap_execute_freeze_tuple` | `src/backend/access/heap/heapam.c` | vacuum | 0.70 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |
| `heap_hot_search_buffer` | `src/backend/access/heap/heapam.c` | tuple | 0.72 | [Deep Dives: HOT](10_deep_dives.md) |
| `heap_insert` | `src/backend/access/heap/heapam.c` | tuple | 0.93 | [Tuple Versioning](04_tuple_versioning.md) |
| `heap_lock_tuple` | `src/backend/access/heap/heapam.c` | tuple | 0.78 | [Tuple Versioning](04_tuple_versioning.md) |
| `heap_page_prune_and_freeze` | `src/backend/access/heap/pruneheap.c` | vacuum | 0.83 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |
| `heap_page_prune_opt` | `src/backend/access/heap/pruneheap.c` | vacuum | 0.70 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |
| `heap_prepare_freeze_tuple` | `src/backend/access/heap/heapam.c` | vacuum | 0.85 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |
| `heap_update` | `src/backend/access/heap/heapam.c` | tuple | 0.93 | [Tuple Versioning](04_tuple_versioning.md) |
| `lazy_vacuum_heap_rel` | `src/backend/access/heap/vacuumlazy.c` | vacuum | 0.88 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |
| `vac_truncate_clog` | `src/backend/commands/vacuum.c` | vacuum | 0.72 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |
| `vacuum_get_cutoffs` | `src/backend/commands/vacuum.c` | vacuum | 0.80 | [VACUUM and Freezing](09_vacuum_and_freezing.md) |

### By Category

| Category | Count | Key Symbols |
|----------|-------|-------------|
| visibility | 14 | `HeapTupleSatisfiesMVCC`, `SetHintBits`, `XidInMVCCSnapshot` |
| transaction | 14 | `StartTransaction`, `CommitTransaction`, `GetNewTransactionId` |
| concurrency | 13 | `PGPROC`, `TransactionIdIsInProgress`, `ProcArrayEndTransaction` |
| snapshot | 11 | `SnapshotData`, `GetSnapshotData`, `GetTransactionSnapshot` |
| clog | 10 | `TransactionIdDidCommit`, `TransactionIdSetTreeStatus` |
| tuple | 8 | `HeapTupleHeaderData`, `heap_insert`, `heap_update`, `heap_delete` |
| vacuum | 10 | `heap_prepare_freeze_tuple`, `lazy_vacuum_heap_rel`, `vacuum_get_cutoffs` |

---

Previous: [Deep Dives](10_deep_dives.md) | Next: [Appendix: Glossary](appendix_glossary.md)
