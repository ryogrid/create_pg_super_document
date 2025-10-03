# parallel_vacuum_bulkdel_all_indexes

## Location
[src/backend/commands/vacuumparallel.c:498-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L498-L516)

## Overview
Coordinates parallel execution of bulk delete operations across all indexes by setting up shared metadata and delegating the actual work to the parallel processing infrastructure.

## Definition

```c
void
parallel_vacuum_bulkdel_all_indexes(ParallelVacuumState *pvs, long num_table_tuples,
									int num_index_scans)
```
## Detailed Description
This function serves as the entry point for parallel bulk deletion operations on indexes. It:

1. **Validation**: Ensures the function is called by the leader process (not a parallel worker)
2. **Metadata Setup**: Stores table tuple count information in shared memory for workers to access
3. **Estimation Flag**: Marks the tuple count as estimated rather than exact, since parallel workers need approximate values for optimization decisions
4. **Delegation**: Calls the general parallel processing function with the bulk delete flag enabled

The function is a lightweight coordinator that prepares shared state and then delegates to the core parallel processing infrastructure.

## Parameters / Member Variables
- `*pvs`: Pointer to the parallel vacuum state structure containing coordination data
- `num_table_tuples`: Approximate number of tuples in the table being vacuumed, used for optimization decisions
- `num_index_scans`: Number of index scan passes planned for the operation
## Dependencies
- Functions called/Symbols referenced:
  -  - Verifies this is running in the leader process
  -  - Core function that coordinates parallel index processing with bulk delete enabled
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:2051)

## Notes and Other Information
- Function is located at src/backend/commands/vacuumparallel.c:498-516
- Must be called only by the leader process, never by parallel workers
- The tuple count is stored as an approximation () because exact counts are not always available or necessary for bulk delete operations
- Serves as a specialized interface to the general parallel vacuum processing infrastructure
- The bulk delete phase is typically the first major parallel operation in vacuum processing, removing dead tuples from index structures