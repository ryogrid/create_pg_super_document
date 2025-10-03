# validate_index

## Location
[src/backend/catalog/index.c:3289-3421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L3289-L3421)

## Overview
Validates a concurrently built index by ensuring all tuples visible in a reference snapshot are present in the index, completing the concurrent index build process.

## Definition
```c
void validate_index(Oid heapId, Oid indexId, Snapshot snapshot)
```

## Detailed Description
validate_index is the final validation phase of concurrent index building. After an index has been built and marked as "indisready" (but not yet "indisvalid"), this function ensures that all tuples visible according to a reference snapshot are present in the index. The validation process uses a sophisticated merge-join approach:

1. **Index Scan Phase**: Gathers all TIDs currently in the index using index_bulk_delete with a callback that stores TIDs without deleting them
2. **Sort Phase**: Sorts the collected TIDs for efficient merging, encoding them as int8 values for better performance
3. **Table Scan Phase**: Performs a table scan and merges it with the sorted index TIDs to identify missing tuples
4. **Insertion Phase**: Any missing tuples are inserted into the index

The function handles security by switching to the table owner's userid and restricting operations. It also provides detailed progress reporting through the PostgreSQL progress reporting system with phases for index scan, sort, and table scan.

## Parameters / Member Variables
- `heapId`: Object identifier of the heap relation being indexed
- `indexId`: Object identifier of the index being validated
- `snapshot`: Reference snapshot defining which tuples should be visible and indexed
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [index_bulk_delete](../i/index_bulk_delete.md)
  - [validate_index_callback](validate_index_callback.md)
  - [table_index_validate_scan](../t/table_index_validate_scan.md)
  - [index_insert_cleanup](../i/index_insert_cleanup.md)
  - [tuplesort_begin_datum](../t/tuplesort_begin_datum.md)
  - [tuplesort_performsort](../t/tuplesort_performsort.md)
  - [tuplesort_end](../t/tuplesort_end.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)

## Notes and Other Information
- This is a critical component of PostgreSQL's concurrent index building feature
- The merge-join strategy avoids the need for a full indexscan, which not all index access methods support
- TIDs are encoded as int8 for sorting performance since TID is pass-by-reference while int8 is pass-by-value
- The function handles unique indexes carefully to avoid false uniqueness violations during concurrent operations
- Security context switching ensures index functions run with appropriate privileges
- Progress reporting provides visibility into long-running validation operations
- Maintains ShareUpdateExclusiveLock on the heap and RowExclusiveLock on the index during validation

## Simplified Source

```c
void
validate_index(Oid heapId, Oid indexId, Snapshot snapshot)
{
    Relation heapRelation, indexRelation;
    IndexInfo *indexInfo;
    IndexVacuumInfo ivinfo;
    ValidateIndexState state;
    Oid save_userid;
    int save_sec_context;
    int save_nestlevel;

    // Update progress reporting
    pgstat_progress_update_multi_param(5, progress_index,
        {PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN, 0, 0, 0, 0});

    // Open and lock relations
    heapRelation = table_open(heapId, ShareUpdateExclusiveLock);
    indexRelation = index_open(indexId, RowExclusiveLock);

    // Switch to table owner's security context
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(heapRelation->rd_rel->relowner,
                           save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    // Build index information structure
    indexInfo = BuildIndexInfo(indexRelation);
    indexInfo->ii_Concurrent = true;

    // Set up index vacuum info
    ivinfo.index = indexRelation;
    ivinfo.heaprel = heapRelation;
    ivinfo.analyze_only = false;
    ivinfo.report_progress = true;
    ivinfo.estimated_count = true;
    ivinfo.message_level = DEBUG2;
    ivinfo.num_heap_tuples = heapRelation->rd_rel->reltuples;
    ivinfo.strategy = NULL;

    // Initialize tuplesort for TID collection
    // Use int8 encoding for better performance than direct TID sorting
    state.tuplesort = tuplesort_begin_datum(INT8OID, Int8LessOperator,
                                            InvalidOid, false,
                                            maintenance_work_mem,
                                            NULL, TUPLESORT_NONE);
    state.htups = state.itups = state.tups_inserted = 0;

    // Phase 1: Collect all TIDs from the index
    (void) index_bulk_delete(&ivinfo, NULL,
                             validate_index_callback, (void *) &state);

    // Phase 2: Sort collected TIDs
    pgstat_progress_update_multi_param(3, progress_index,
        {PROGRESS_CREATEIDX_PHASE_VALIDATE_SORT, 0, 0});
    tuplesort_performsort(state.tuplesort);

    // Phase 3: Scan heap and merge with index TIDs
    pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE,
                                 PROGRESS_CREATEIDX_PHASE_VALIDATE_TABLESCAN);
    table_index_validate_scan(heapRelation, indexRelation, indexInfo,
                              snapshot, &state);

    // Clean up tuplesort
    tuplesort_end(state.tuplesort);

    // Release index resources
    index_insert_cleanup(indexRelation, indexInfo);

    elog(DEBUG2,
         "validate_index found %.0f heap tuples, %.0f index tuples; inserted %.0f missing tuples",
         state.htups, state.itups, state.tups_inserted);

    // Restore security context and GUC settings
    AtEOXact_GUC(false, save_nestlevel);
    SetUserIdAndSecContext(save_userid, save_sec_context);

    // Close relations but keep locks
    index_close(indexRelation, NoLock);
    table_close(heapRelation, NoLock);
}
```