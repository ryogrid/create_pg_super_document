# parallel_vacuum_end

## Location
[src/backend/commands/vacuumparallel.c:434-464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L434-L464)

## Overview
Cleanly terminates parallel vacuum execution by copying statistics from shared memory to local memory, destroying the parallel context, and freeing all allocated resources.

## Definition


## Detailed Description
This function performs the orderly shutdown of parallel vacuum operations by:

1. **Statistics Preservation**: Copies updated index bulk delete statistics from shared memory (DSM) to local memory before the parallel context is destroyed, ensuring statistics are preserved for later use
2. **Resource Cleanup**: Destroys the shared TidStore used for dead tuple tracking
3. **Parallel Context Teardown**: Destroys the parallel context and exits parallel mode
4. **Memory Deallocation**: Frees the parallel vacuum state structure and associated arrays

The function must run in the leader process (not a parallel worker) and carefully handles the order of operations since writes are not allowed during parallel mode. Statistics must be copied before exiting parallel mode to avoid unsafe operations.

## Parameters / Member Variables
- : Pointer to the parallel vacuum state structure to be cleaned up
- : Output array of index bulk delete result pointers; filled with copied statistics from workers, or NULL for indexes that weren't updated

## Dependencies
- Functions called/Symbols referenced:
  -  - Verifies this is running in the leader process
  -  - Destroys the shared dead tuple storage
  -  - Tears down the parallel worker context
  -  - Exits parallel execution mode
  -  /  - Memory allocation/deallocation functions
  -  - Copies statistics from shared to local memory
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:2939)

## Notes and Other Information
- Function is located at src/backend/commands/vacuumparallel.c:434-464
- Must be called only by the leader process, never by parallel workers
- Critical ordering: statistics must be copied before destroying parallel context due to write restrictions in parallel mode
- The  array is populated with statistics only for indexes that were actually updated during parallel processing
- All dynamically allocated memory from the parallel vacuum state is properly freed to prevent memory leaks