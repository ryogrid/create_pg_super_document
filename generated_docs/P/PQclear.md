# PQclear

## Location
[src/interfaces/libpq/fe-exec.c:721-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L721-L778)

## Overview
PQclear is a PostgreSQL libpq library function that frees the memory associated with a PGresult structure, providing proper cleanup and resource management for query results.

## Definition

```c
structure itself */
	free(res);
```
## Detailed Description
PQclear performs comprehensive cleanup of a PGresult structure and all its associated memory. The function handles several key cleanup tasks:

1. **Safety checks**: Returns immediately if the result pointer is NULL or points to the special OOM_result constant
2. **Event cleanup**: Destroys any registered event handlers by calling their PGEVT_RESULTDESTROY callbacks
3. **Memory block cleanup**: Frees all subsidiary data blocks linked to the result
4. **Tuple data cleanup**: Frees the tuple pointer array containing row data
5. **Structure cleanup**: Zeros out pointer fields to catch programming errors and frees the main PGresult structure

The function is designed to be safe to call multiple times and handles edge cases gracefully. It's a critical part of libpq's memory management system.

## Parameters / Member Variables
- : Pointer to the PGresult structure to be freed. Can be NULL (no-op) or point to OOM_result (no-op)

## Dependencies
- Functions called/Symbols referenced:
  - PGresult_data (data structure for memory blocks)
  - PGEventResultDestroy (event structure for destruction callbacks)
  - PGEVT_RESULTDESTROY (event type constant)
  - free() (standard library memory deallocation)

- Called from (representative examples):
  - Various libpq client applications
  - Other libpq internal functions that need to clean up results

## Notes and Other Information
- **Memory safety**: The function zeroes out pointer fields after freeing to help catch use-after-free bugs
- **Event system**: Properly handles the libpq event system by calling destruction callbacks for all registered events
- **OOM handling**: Special handling for the singleton OOM_result prevents attempting to free static memory
- **Null safety**: Designed as a convenience function that safely handles NULL input
- **Resource management**: This is the primary cleanup function for PGresult objects and should be called for every result obtained from libpq functions

## Simplified Source

```c
void PQclear(PGresult *res) {
    // Safety check: do nothing for NULL or special OOM_result
    if (!res || res == &OOM_result)
        return;

    // Clean up event handlers
    for (int i = 0; i < res->nEvents; i++) {
        if (res->events[i].resultInitialized) {
            // Call destroy callback for initialized events
            PGEventResultDestroy evt = {.result = res};
            res->events[i].proc(PGEVT_RESULTDESTROY, &evt, res->events[i].passThrough);
        }
        free(res->events[i].name);
    }
    free(res->events);

    // Free all memory blocks
    PGresult_data *block;
    while ((block = res->curBlock) != NULL) {
        res->curBlock = block->next;
        free(block);
    }

    // Free tuple data
    free(res->tuples);

    // Zero out pointers to catch programming errors
    res->attDescs = NULL;
    res->tuples = NULL;
    res->paramDescs = NULL;
    res->errFields = NULL;
    res->events = NULL;
    res->nEvents = 0;

    // Free the main structure
    free(res);
}
```