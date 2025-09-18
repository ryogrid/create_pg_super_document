# ParallelSlotsWaitCompletion

## Location
src/fe_utils/parallel_slot.c: 501 - 539

## Overview
Waits for all active connections in a parallel slots array to complete their current operations, returning false if any errors are encountered during processing.

## Definition


## Detailed Description
ParallelSlotsWaitCompletion synchronously waits for all active database connections in the parallel slots array to finish their current operations. It iterates through each slot, consuming query results from active connections and handling any errors that occur.

For each slot with an active connection, the function calls consumeQueryResult to process the query results. If any connection encounters an error during result processing, the function immediately returns false. Upon successful completion of a slot's operation, the slot is marked as not in use (inUse = false) and its handler is cleared via ParallelSlotClearHandler.

This function is essential for ensuring all parallel operations complete before proceeding to the next phase of execution or cleanup.

## Parameters / Member Variables
- : Pointer to the ParallelSlotArray structure containing the connections to wait for completion

## Dependencies
- Functions called/Symbols referenced:
  - consumeQueryResult
  - ParallelSlotClearHandler
  - ParallelSlotArray
- Called from (representative examples):
  - main (src/bin/pg_amcheck/pg_amcheck.c:806)
  - reindex_one_database (src/bin/scripts/reindexdb.c:478)
  - vacuum_one_database (src/bin/scripts/vacuumdb.c:866, 887)
  - ParallelSlotClearHandler (src/include/fe_utils/parallel_slot.h:72)

## Notes and Other Information
- Returns true if all operations completed successfully, false if any errors occurred
- Marks completed slots as not in use (inUse = false) for potential reuse
- Clears slot handlers after successful completion
- Skips null connections (empty slots) during iteration
- Essential for synchronization in parallel database operations
- Located in src/fe_utils/parallel_slot.c:501-539