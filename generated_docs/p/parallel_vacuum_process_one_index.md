# parallel_vacuum_process_one_index

## Location
[src/backend/commands/vacuumparallel.c:863-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L863-L948)

## Overview
Processes a single index during parallel vacuum operation, performing either bulk deletion or cleanup depending on the index status, and updates shared statistics in DSM segment.

## Definition

```c
static void
parallel_vacuum_process_one_index(ParallelVacuumState *pvs, Relation indrel,
								  PVIndStats *indstats)
```
## Detailed Description
This function is the core worker routine for processing individual indexes during parallel vacuum operations. It handles both the bulk deletion phase (removing dead tuples) and the cleanup phase (finalizing index state) of vacuum processing. The function operates on indexes that have been determined to be safe for parallel processing and coordinates with other worker processes through shared memory structures.

The function performs different operations based on the index status:
- **PARALLEL_INDVAC_STATUS_NEED_BULKDELETE**: Calls  to remove dead tuple references
- **PARALLEL_INDVAC_STATUS_NEED_CLEANUP**: Calls  to finalize index state

Key design aspects include:
- **Shared Statistics Management**: Copies index bulk-deletion results to DSM segment for coordination between processes
- **Error Tracking**: Updates parallel vacuum state with current index name and status for error reporting
- **Progress Reporting**: Reports progress to the leader process via parallel progress tracking
- **Lock-free Updates**: Each worker operates on different index slots, avoiding contention

## Parameters / Member Variables
- `*pvs`: Parallel vacuum state containing shared memory structures, heap relation, buffer strategy, and coordination data
- `indrel`: The index relation being processed
- `*indstats`: Per-index statistics structure in shared memory containing status and bulk-deletion results
## Dependencies
- Functions called/Symbols referenced:
  - [vac_bulkdel_one_index](../v/vac_bulkdel_one_index.md)
  - [vac_cleanup_one_index](../v/vac_cleanup_one_index.md)
  - [pgstat_progress_parallel_incr_param](pgstat_progress_parallel_incr_param.md)
  - RelationGetRelationName
  - [pstrdup](pstrdup.md)
  - [pfree](pfree.md)
  - memcpy
  - elog
- Called from (representative examples):
  - [parallel_vacuum_process_safe_indexes](parallel_vacuum_process_safe_indexes.md)
  - [parallel_vacuum_process_unsafe_indexes](parallel_vacuum_process_unsafe_indexes.md)

## Notes and Other Information
- This is a static function used internally within the parallel vacuum implementation
- The function handles both first-time index processing (copying results to DSM) and subsequent cycles (reusing DSM results)
- Error traceback information is maintained throughout the operation to provide meaningful error messages
- Progress reporting uses the parallel variant to ensure proper coordination with the leader process
- The function operates without locks on index statistics since each worker processes different indexes

## Simplified Source

```c
static void
parallel_vacuum_process_one_index(ParallelVacuumState *pvs, Relation indrel,
                                  PVIndStats *indstats)
{
    IndexBulkDeleteResult *istat = NULL;
    IndexBulkDeleteResult *istat_res;
    IndexVacuumInfo ivinfo;

    // Use existing bulk-deletion result if available
    if (indstats->istat_updated)
        istat = &(indstats->istat);

    // Setup index vacuum info structure
    ivinfo.index = indrel;
    ivinfo.heaprel = pvs->heaprel;
    ivinfo.analyze_only = false;
    ivinfo.report_progress = false;
    ivinfo.message_level = DEBUG2;
    ivinfo.estimated_count = pvs->shared->estimated_count;
    ivinfo.num_heap_tuples = pvs->shared->reltuples;
    ivinfo.strategy = pvs->bstrategy;

    // Update error context for this index
    pvs->indname = pstrdup(RelationGetRelationName(indrel));
    pvs->status = indstats->status;

    // Perform index operation based on status
    switch (indstats->status)
    {
        case PARALLEL_INDVAC_STATUS_NEED_BULKDELETE:
            istat_res = vac_bulkdel_one_index(&ivinfo, istat, pvs->dead_items,
                                              &pvs->shared->dead_items_info);
            break;
        case PARALLEL_INDVAC_STATUS_NEED_CLEANUP:
            istat_res = vac_cleanup_one_index(&ivinfo, istat);
            break;
        default:
            elog(ERROR, "unexpected parallel vacuum index status %d for index \"%s\"",
                 indstats->status, RelationGetRelationName(indrel));
    }

    // Copy result to shared memory on first cycle
    if (!indstats->istat_updated && istat_res != NULL)
    {
        memcpy(&(indstats->istat), istat_res, sizeof(IndexBulkDeleteResult));
        indstats->istat_updated = true;
        pfree(istat_res);
    }

    // Mark index as completed
    indstats->status = PARALLEL_INDVAC_STATUS_COMPLETED;

    // Reset error context
    pvs->status = PARALLEL_INDVAC_STATUS_COMPLETED;
    pfree(pvs->indname);
    pvs->indname = NULL;

    // Report progress to leader
    pgstat_progress_parallel_incr_param(PROGRESS_VACUUM_INDEXES_PROCESSED, 1);
}
```