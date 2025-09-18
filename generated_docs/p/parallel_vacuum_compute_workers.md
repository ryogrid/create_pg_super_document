# parallel_vacuum_compute_workers

## Location
src/backend/commands/vacuumparallel.c: 547 - 608

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
  - parallel_vacuum_init

## Notes and Other Information
- Returns 0 if parallel vacuum is not possible (standalone mode, no suitable indexes, etc.)
- The function distinguishes between bulkdel and cleanup operations when counting eligible indexes
- Indexes must be larger than min_parallel_index_scan_size to be considered for parallel processing
- The final worker count is the minimum of: requested workers, eligible indexes (minus 1 for leader), and max_parallel_maintenance_workers
- The will_parallel_vacuum array is populated as a side effect to indicate which indexes will participate