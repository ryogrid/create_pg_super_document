# lazy_vacuum_all_indexes

## Location
[src/backend/access/heap/vacuumlazy.c:1990-2106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1990-L2106)

## Overview
Main entry point for index vacuuming that coordinates the removal of dead tuple references from all indexes, supporting both serial and parallel execution modes.

## Definition
```c
static bool lazy_vacuum_all_indexes(LVRelState *vacrel)
```

## Detailed Description
lazy_vacuum_all_indexes orchestrates the index vacuuming phase of the VACUUM operation, responsible for removing references to dead tuples from all indexes on the relation. The function implements comprehensive wraparound failsafe checking before, during, and after index processing to prevent transaction ID wraparound failures. It supports both serial execution (processing indexes one by one) and parallel execution (delegating to parallel vacuum workers). The function maintains detailed progress reporting for monitoring and includes sophisticated error handling for emergency scenarios where the vacuum operation must be terminated early to prevent system-wide transaction ID wraparound.

## Parameters / Member Variables
- `vacrel`: LVRelState containing the complete VACUUM operation state, including index information, dead items, and execution configuration

## Dependencies
- Functions called/Symbols referenced:
  - [lazy_check_wraparound_failsafe](lazy_check_wraparound_failsafe.md)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
  - ParallelVacuumIsActive
  - [lazy_vacuum_one_index](lazy_vacuum_one_index.md)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [parallel_vacuum_bulkdel_all_indexes](../p/parallel_vacuum_bulkdel_all_indexes.md)
  - PROGRESS_VACUUM_PHASE
  - PROGRESS_VACUUM_INDEXES_TOTAL
  - PROGRESS_VACUUM_INDEXES_PROCESSED
  - PROGRESS_VACUUM_NUM_INDEX_VACUUMS
  - PROGRESS_VACUUM_PHASE_VACUUM_INDEX
- Called from:
  - [lazy_vacuum](lazy_vacuum.md)

## Notes and Other Information
- Returns true if all indexes were successfully vacuumed, false if wraparound failsafe triggered
- Performs multiple wraparound failsafe checks: pre-check, during processing, and post-check
- Updates progress reporting with detailed index processing metrics
- For parallel vacuum, delegates bulk delete operations to parallel_vacuum_bulkdel_all_indexes
- Increments num_index_scans counter even for incomplete rounds due to failsafe activation
- Serial mode processes indexes sequentially with per-index wraparound checking
- Maintains IndexBulkDeleteResult statistics for each index vacuum operation
- Critical for preventing transaction ID wraparound by monitoring vacuum duration and progress

## Simplified Source

```c
static bool
lazy_vacuum_all_indexes(LVRelState *vacrel)
{
    bool allindexes = true;
    double old_live_tuples = vacrel->rel->rd_rel->reltuples;

    Assert(vacrel->nindexes > 0);
    Assert(vacrel->do_index_vacuuming);
    Assert(vacrel->do_index_cleanup);

    // Pre-check for wraparound emergency
    if (lazy_check_wraparound_failsafe(vacrel))
        return false;

    // Report progress to stats system
    pgstat_progress_update_multi_param(2,
        (int[]){PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_INDEXES_TOTAL},
        (int64[]){PROGRESS_VACUUM_PHASE_VACUUM_INDEX, vacrel->nindexes});

    if (!ParallelVacuumIsActive(vacrel))
    {
        // Serial execution - vacuum each index
        for (int idx = 0; idx < vacrel->nindexes; idx++)
        {
            Relation indrel = vacrel->indrels[idx];
            IndexBulkDeleteResult *istat = vacrel->indstats[idx];

            // Vacuum this index
            vacrel->indstats[idx] = lazy_vacuum_one_index(indrel, istat,
                                                         old_live_tuples, vacrel);

            // Update progress
            pgstat_progress_update_param(PROGRESS_VACUUM_INDEXES_PROCESSED, idx + 1);

            // Check for wraparound emergency after each index
            if (lazy_check_wraparound_failsafe(vacrel))
            {
                allindexes = false;
                break;
            }
        }
    }
    else
    {
        // Parallel execution - delegate to parallel workers
        parallel_vacuum_bulkdel_all_indexes(vacrel->pvs, old_live_tuples,
                                           vacrel->num_index_scans);

        // Post-check for wraparound in parallel mode
        if (lazy_check_wraparound_failsafe(vacrel))
            allindexes = false;
    }

    // Update scan count and reset progress counters
    vacrel->num_index_scans++;
    pgstat_progress_update_multi_param(3,
        (int[]){PROGRESS_VACUUM_INDEXES_TOTAL, PROGRESS_VACUUM_INDEXES_PROCESSED,
               PROGRESS_VACUUM_NUM_INDEX_VACUUMS},
        (int64[]){0, 0, vacrel->num_index_scans});

    return allindexes;
}
```