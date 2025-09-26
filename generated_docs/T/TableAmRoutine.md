# TableAmRoutine

## Location
[src/include/access/tableam.h:291-877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L291-L877)

## Overview
TableAmRoutine is a comprehensive structure that defines the API interface for table access methods in PostgreSQL, containing function pointers for all operations that a table access method must implement.

## Definition
```c
typedef struct TableAmRoutine
{
    /* this must be set to T_TableAmRoutine */
    NodeTag     type;
    
    /* Slot related callbacks */
    const TupleTableSlotOps *(*slot_callbacks) (Relation rel);
    
    /* Table scan callbacks */
    TableScanDesc (*scan_begin) (Relation rel, Snapshot snapshot, int nkeys, 
                                struct ScanKeyData *key, ParallelTableScanDesc pscan, uint32 flags);
    void        (*scan_end) (TableScanDesc scan);
    void        (*scan_rescan) (TableScanDesc scan, struct ScanKeyData *key, 
                               bool set_params, bool allow_strat, bool allow_sync, bool allow_pagemode);
    bool        (*scan_getnextslot) (TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
    
    /* Optional TID range scanning */
    void        (*scan_set_tidrange) (TableScanDesc scan, ItemPointer mintid, ItemPointer maxtid);
    bool        (*scan_getnextslot_tidrange) (TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot);
    
    /* Parallel table scan callbacks */
    Size        (*parallelscan_estimate) (Relation rel);
    Size        (*parallelscan_initialize) (Relation rel, ParallelTableScanDesc pscan);
    void        (*parallelscan_reinitialize) (Relation rel, ParallelTableScanDesc pscan);
    
    /* Index scan callbacks */  
    struct IndexFetchTableData *(*index_fetch_begin) (Relation rel);
    void        (*index_fetch_reset) (struct IndexFetchTableData *data);
    void        (*index_fetch_end) (struct IndexFetchTableData *data);
    bool        (*index_fetch_tuple) (struct IndexFetchTableData *scan, ItemPointer tid,
                                     Snapshot snapshot, TupleTableSlot *slot, bool *call_again, bool *all_dead);
    
    /* Non-modifying tuple operations */
    bool        (*tuple_fetch_row_version) (Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot);
    bool        (*tuple_tid_valid) (TableScanDesc scan, ItemPointer tid);
    void        (*tuple_get_latest_tid) (TableScanDesc scan, ItemPointer tid);
    bool        (*tuple_satisfies_snapshot) (Relation rel, TupleTableSlot *slot, Snapshot snapshot);
    
    /* Index deletion callback */
    TransactionId (*index_delete_tuples) (Relation rel, TM_IndexDeleteOp *delstate);
    
    /* Physical tuple manipulation */
    void        (*tuple_insert) (Relation rel, TupleTableSlot *slot, CommandId cid, 
                                int options, struct BulkInsertStateData *bistate);
    void        (*tuple_insert_speculative) (Relation rel, TupleTableSlot *slot, CommandId cid,
                                            int options, struct BulkInsertStateData *bistate, uint32 specToken);
    void        (*tuple_complete_speculative) (Relation rel, TupleTableSlot *slot, uint32 specToken, bool succeeded);
    void        (*multi_insert) (Relation rel, TupleTableSlot **slots, int nslots, CommandId cid,
                                int options, struct BulkInsertStateData *bistate);
    TM_Result   (*tuple_delete) (Relation rel, ItemPointer tid, CommandId cid, Snapshot snapshot,
                                Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart);
    TM_Result   (*tuple_update) (Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
                                Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
                                LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes);
    TM_Result   (*tuple_lock) (Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
                              CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
                              uint8 flags, TM_FailureData *tmfd);
    void        (*finish_bulk_insert) (Relation rel, int options);
    
    /* DDL related functionality */
    void        (*relation_set_new_filelocator) (Relation rel, const RelFileLocator *newrlocator,
                                                char persistence, TransactionId *freezeXid, MultiXactId *minmulti);
    void        (*relation_nontransactional_truncate) (Relation rel);
    void        (*relation_copy_data) (Relation rel, const RelFileLocator *newrlocator);
    void        (*relation_copy_for_cluster) (Relation OldTable, Relation NewTable, Relation OldIndex,
                                             bool use_sort, TransactionId OldestXmin, TransactionId *xid_cutoff,
                                             MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed, double *tups_recently_dead);
    void        (*relation_vacuum) (Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy);
    
    /* ANALYZE support */
    bool        (*scan_analyze_next_block) (TableScanDesc scan, ReadStream *stream);
    bool        (*scan_analyze_next_tuple) (TableScanDesc scan, TransactionId OldestXmin,
                                           double *liverows, double *deadrows, TupleTableSlot *slot);
    
    /* Index building */
    double      (*index_build_range_scan) (Relation table_rel, Relation index_rel, struct IndexInfo *index_info,
                                          bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno,
                                          BlockNumber numblocks, IndexBuildCallback callback, void *callback_state, TableScanDesc scan);
    void        (*index_validate_scan) (Relation table_rel, Relation index_rel, struct IndexInfo *index_info,
                                       Snapshot snapshot, struct ValidateIndexState *state);
    
    /* Miscellaneous */
    uint64      (*relation_size) (Relation rel, ForkNumber forkNumber);
    bool        (*relation_needs_toast_table) (Relation rel);
    Oid         (*relation_toast_am) (Relation rel);
    void        (*relation_fetch_toast_slice) (Relation toastrel, Oid valueid, int32 attrsize,
                                              int32 sliceoffset, int32 slicelength, struct varlena *result);
    
    /* Planner support */
    void        (*relation_estimate_size) (Relation rel, int32 *attr_widths, BlockNumber *pages,
                                          double *tuples, double *allvisfrac);
    
    /* Executor support */
    bool        (*scan_bitmap_next_block) (TableScanDesc scan, struct TBMIterateResult *tbmres);
    bool        (*scan_bitmap_next_tuple) (TableScanDesc scan, struct TBMIterateResult *tbmres, TupleTableSlot *slot);
    bool        (*scan_sample_next_block) (TableScanDesc scan, struct SampleScanState *scanstate);
    bool        (*scan_sample_next_tuple) (TableScanDesc scan, struct SampleScanState *scanstate, TupleTableSlot *slot);
} TableAmRoutine;
```

## Detailed Description
TableAmRoutine is the cornerstone of PostgreSQL's pluggable table access method architecture. It defines a comprehensive interface that allows different storage engines to be plugged into PostgreSQL while maintaining compatibility with the existing query processing infrastructure. This structure must be allocated in a server-lifetime manner (typically as a static const struct) and is returned by the FormData_pg_am.amhandler function.

The structure provides a complete abstraction layer between PostgreSQL's upper layers (query planning, execution, utilities) and the underlying storage implementation. Each table access method (like heap, or potentially columnar stores, etc.) implements this interface to provide its specific storage and retrieval mechanisms.

The callbacks are organized into logical groups covering all aspects of table management: scanning, index operations, tuple manipulation, DDL operations, maintenance tasks, and optimization support.

## Parameters / Member Variables
- `type`: NodeTag that must be set to T_TableAmRoutine for type identification
- `slot_callbacks`: Returns appropriate TupleTableSlot implementation for the AM
- Scan callbacks (scan_begin, scan_end, scan_rescan, scan_getnextslot): Core sequential scanning functionality
- TID range callbacks (scan_set_tidrange, scan_getnextslot_tidrange): Optional TID-based range scanning
- Parallel scan callbacks: Support for parallel table scanning operations
- Index scan callbacks: Support for index-driven tuple retrieval
- Tuple operation callbacks: Non-modifying tuple access and validation
- `index_delete_tuples`: Performs index tuple deletion coordination using TM_IndexDeleteOp
- Modification callbacks (tuple_insert, tuple_update, tuple_delete, tuple_lock): Core CRUD operations
- DDL callbacks: Support for schema and storage management operations
- Maintenance callbacks (relation_vacuum, analyze support): Support for database maintenance
- Utility callbacks: Size estimation, TOAST support, planner integration
- Executor callbacks: Support for specialized scan types (bitmap, sample)

## Dependencies
- Functions called/Symbols referenced:
  - TM_IndexDeleteOp (for index deletion operations)
  - TM_FailureData (for operation failure reporting)
  - Multiple PostgreSQL core types (Relation, Snapshot, CommandId, etc.)
- Called from (representative examples):
  - GetTableAmRoutine
  - table_tuple_get_latest_tid  
  - SampleHeapTupleVisible
  - RelationData (embedded reference)

## Notes and Other Information
- Central to PostgreSQL's extensible storage architecture, enabling pluggable table access methods
- Must implement all required callbacks; GetTableAmRoutine() validates completeness
- Generally accessed through table_* wrapper functions rather than direct callback invocation
- Enables support for different storage formats while maintaining SQL compatibility
- The heap access method (heapam) is the default implementation of this interface
- Critical for PostgreSQL's ability to support diverse workloads through specialized storage engines
- Designed to support both block-oriented and non-block-oriented storage implementations
- Provides hooks for integration with PostgreSQL's MVCC, transaction management, and query optimization systems