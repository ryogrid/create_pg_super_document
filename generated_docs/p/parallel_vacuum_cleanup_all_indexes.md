# parallel_vacuum_cleanup_all_indexes

## Location
[src/backend/commands/vacuumparallel.c:517-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L517-L546)

## Overview
This function performs parallel index cleanup using parallel workers during the cleanup phase of vacuum operations.

## Definition

```c
void
parallel_vacuum_cleanup_all_indexes(ParallelVacuumState *pvs, long num_table_tuples,
									int num_index_scans, bool estimated_count)
```
## Detailed Description
The function coordinates parallel index cleanup operations across multiple worker processes. It updates the shared state with the number of surviving tuples and whether the count is estimated, then delegates the actual parallel processing to . This function is specifically designed for the cleanup phase where indexes need to be processed after the main vacuum operation has completed.

The function assumes that indexes are more interested in the number of surviving tuples rather than nominally live tuples, and provides this information to help with better cleanup estimates.

## Parameters / Member Variables
- : ParallelVacuumState pointer containing the shared state for parallel vacuum operations
- : The number of surviving tuples in the table after vacuum
- : The number of index scans to be performed during cleanup
- : Boolean indicating whether the tuple count is estimated or exact

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - [parallel_vacuum_process_all_indexes](parallel_vacuum_process_all_indexes.md)
  - [ParallelVacuumState](../P/ParallelVacuumState.md)
- Called from (representative examples):
  - [lazy_cleanup_all_indexes](../l/lazy_cleanup_all_indexes.md)

## Notes and Other Information
- This function must not be called from a parallel worker (enforced by Assert(!IsParallelWorker()))
- The function updates the shared state's reltuples and estimated_count fields before delegating to the general parallel processing function
- It's specifically designed for cleanup operations (as opposed to vacuum operations) as indicated by the false parameter passed to parallel_vacuum_process_all_indexes