# ParallelSlotsWaitCompletion

## Location
[src/fe_utils/parallel_slot.c:501-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/parallel_slot.c#L501-L539)

## Overview
Waits for all active connections in a parallel slots array to complete their current operations, returning false if any errors are encountered during processing.

## Definition

```c
bool
ParallelSlotsWaitCompletion(ParallelSlotArray *sa)
```
## Detailed Description
ParallelSlotsWaitCompletion synchronously waits for all active database connections in the parallel slots array to finish their current operations. It iterates through each slot, consuming query results from active connections and handling any errors that occur.

For each slot with an active connection, the function calls consumeQueryResult to process the query results. If any connection encounters an error during result processing, the function immediately returns false. Upon successful completion of a slot's operation, the slot is marked as not in use (inUse = false) and its handler is cleared via ParallelSlotClearHandler.

This function is essential for ensuring all parallel operations complete before proceeding to the next phase of execution or cleanup.

## Parameters / Member Variables
- `*sa`: Pointer to the ParallelSlotArray structure containing the connections to wait for completion
## Dependencies
- Functions called/Symbols referenced:
  - [consumeQueryResult](../c/consumeQueryResult.md)
  - [ParallelSlotClearHandler](ParallelSlotClearHandler.md)
  - [ParallelSlotArray](ParallelSlotArray.md)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_amcheck/pg_amcheck.c:806)
  - [reindex_one_database](../r/reindex_one_database.md) (src/bin/scripts/reindexdb.c:478)
  - [vacuum_one_database](../v/vacuum_one_database.md) (src/bin/scripts/vacuumdb.c:866, 887)
  - [ParallelSlotClearHandler](ParallelSlotClearHandler.md) (src/include/fe_utils/parallel_slot.h:72)

## Notes and Other Information
- Returns true if all operations completed successfully, false if any errors occurred
- Marks completed slots as not in use (inUse = false) for potential reuse
- Clears slot handlers after successful completion
- Skips null connections (empty slots) during iteration
- Essential for synchronization in parallel database operations
- Located in src/fe_utils/parallel_slot.c:501-539

## Simplified Source

```c
bool
ParallelSlotsWaitCompletion(ParallelSlotArray *sa)
{
    int i;

    // Wait for each slot to complete
    for (i = 0; i < sa->numslots; i++)
    {
        if (sa->slots[i].connection == NULL)
            continue; // Skip empty slots

        // Process query results
        if (!consumeQueryResult(&sa->slots[i]))
            return false; // Error occurred

        // Mark slot as available and clear handler
        sa->slots[i].inUse = false;
        ParallelSlotClearHandler(&sa->slots[i]);
    }

    return true; // All slots completed successfully
}
```