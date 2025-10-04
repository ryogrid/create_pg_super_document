# parallel_vacuum_compute_workers

## Location
[src/backend/commands/vacuumparallel.c:547-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L547-L608)

## Overview
This static function computes the optimal number of parallel worker processes to request for parallel vacuum operations based on index characteristics and system constraints.

## Definition
```c
static int parallel_vacuum_compute_workers(Relation *indrels, int nindexes, int nrequested,
                                         bool *will_parallel_vacuum)
```

## Detailed Description
This function determines how many parallel workers should be used for vacuum operations by analyzing the indexes in the relation. It evaluates each index's suitability for parallel processing based on size thresholds and access method capabilities. The function considers both bulkdel (tuple deletion) and cleanup operations, and ensures the computed worker count doesn't exceed system limits.

The function implements several important constraints: it won't allow parallel operations in standalone backend mode, requires indexes to be larger than min_parallel_index_scan_size, and caps the worker count by max_parallel_maintenance_workers. It also accounts for the leader process taking one index, so the parallel worker count is reduced accordingly.

## Parameters / Member Variables
- `indrels`: Array of Relation pointers representing the indexes to be processed
- `nindexes`: The total number of indexes in the indrels array
- `nrequested`: The number of parallel workers explicitly requested by the user (0 for automatic computation)
- `will_parallel_vacuum`: Boolean array (output parameter) indicating which indexes will participate in parallel vacuum

## Dependencies
- Functions called/Symbols referenced:
  - IsUnderPostmaster
  - RelationGetNumberOfBlocks
  - VACUUM_OPTION_NO_PARALLEL
  - VACUUM_OPTION_PARALLEL_BULKDEL
  - VACUUM_OPTION_PARALLEL_CLEANUP
  - VACUUM_OPTION_PARALLEL_COND_CLEANUP
  - max_parallel_maintenance_workers
  - min_parallel_index_scan_size
- Called from (representative examples):
  - [parallel_vacuum_init](parallel_vacuum_init.md)

## Notes and Other Information
- Returns 0 if parallel vacuum is not possible (standalone mode, no suitable indexes, etc.)
- The function distinguishes between bulkdel and cleanup operations when counting eligible indexes
- Indexes must be larger than min_parallel_index_scan_size to be considered for parallel processing
- The final worker count is the minimum of: requested workers, eligible indexes (minus 1 for leader), and max_parallel_maintenance_workers
- The will_parallel_vacuum array is populated as a side effect to indicate which indexes will participate

## Simplified Source

```c
static int
parallel_vacuum_compute_workers(Relation *indrels, int nindexes, int nrequested,
                               bool *will_parallel_vacuum)
{
    int nindexes_parallel_bulkdel = 0;
    int nindexes_parallel_cleanup = 0;
    int parallel_workers;

    // Don't allow parallel in standalone mode or when disabled
    if (!IsUnderPostmaster || max_parallel_maintenance_workers == 0)
        return 0;

    // Count indexes eligible for parallel processing
    for (int i = 0; i < nindexes; i++) {
        Relation indrel = indrels[i];
        uint8 vacoptions = indrel->rd_indam->amparallelvacuumoptions;

        // Skip if index doesn't support parallel or is too small
        if (vacoptions == VACUUM_OPTION_NO_PARALLEL ||
            RelationGetNumberOfBlocks(indrel) < min_parallel_index_scan_size)
            continue;

        will_parallel_vacuum[i] = true;

        // Count by operation type
        if (vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL)
            nindexes_parallel_bulkdel++;
        if ((vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP) ||
            (vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP))
            nindexes_parallel_cleanup++;
    }

    // Use the maximum count for either phase
    int nindexes_parallel = Max(nindexes_parallel_bulkdel, nindexes_parallel_cleanup);

    // Leader takes one index, so subtract it
    nindexes_parallel--;

    // No suitable indexes for parallel processing
    if (nindexes_parallel <= 0)
        return 0;

    // Compute final worker count
    parallel_workers = (nrequested > 0) ?
        Min(nrequested, nindexes_parallel) : nindexes_parallel;

    // Cap by system limit
    parallel_workers = Min(parallel_workers, max_parallel_maintenance_workers);

    return parallel_workers;
}
```