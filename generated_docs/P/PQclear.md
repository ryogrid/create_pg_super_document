# PQclear

## Location
[src/interfaces/libpq/fe-exec.c:721-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L721-L778)

## Overview
PQclear is a PostgreSQL libpq library function that frees the memory associated with a PGresult structure, providing proper cleanup and resource management for query results.

## Definition


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