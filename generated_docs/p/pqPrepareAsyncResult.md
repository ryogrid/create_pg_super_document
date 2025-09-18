# pqPrepareAsyncResult

## Location
src/interfaces/libpq/fe-exec.c: 851 - 937

## Overview
pqPrepareAsyncResult is an internal libpq function that prepares the current asynchronous result object for return to the caller, ensuring a valid PGresult is always returned even under error or out-of-memory conditions.

## Definition
```c
PGresult *pqPrepareAsyncResult(PGconn *conn)
```

## Detailed Description
pqPrepareAsyncResult is a critical function in libpq's result management system that ensures callers always receive a valid PGresult object, even when errors occur or memory is exhausted. The function handles several scenarios:

1. **Existing result processing**: If conn->result already exists and is a FATAL_ERROR, it updates the errorReported position to track how much error text has been shown to the application

2. **Error result creation**: When no result exists, it creates a new error result using the content of conn->errorMessage, handling the case where error_result flag indicates an error condition

3. **Out-of-memory handling**: If memory allocation fails when creating a PGresult, it falls back to the static OOM_result singleton to ensure a non-NULL return value

4. **State management**: Restores saved results in partial-result mode and cleans up connection state

The function implements sophisticated error text tracking to avoid showing duplicate error messages to applications and ensures proper state transitions during asynchronous operations.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure containing the result state to be prepared

## Dependencies
- Functions called/Symbols referenced:
  - PGRES_FATAL_ERROR (result status constant for fatal errors)
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md) (creates new empty result structures)
  - PGRES_EMPTY_QUERY (temporary result status during creation)
  - [pqSetResultError](pqSetResultError.md) (sets error information in result objects)
  - unconstify (macro for casting away const qualifiers)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (appends error messages to connection)

- Called from (representative examples):
  - [PQgetResult](../P/PQgetResult.md) (multiple locations during result retrieval)
  - [getCopyResult](../g/getCopyResult.md) (during COPY operation result handling)
  - [pqFunctionCall3](pqFunctionCall3.md) (during function call result processing)

## Notes and Other Information
- **Internal function**: This is an internal libpq function, not part of the public API
- **Non-NULL guarantee**: The function guarantees to never return NULL, even under out-of-memory conditions
- **Error tracking**: Implements sophisticated tracking of how much error text has been reported to prevent duplication
- **Memory safety**: Provides graceful degradation when memory allocation fails by using the static OOM_result
- **State management**: Handles complex state transitions in partial-result mode and saved result scenarios
- **Asynchronous support**: Critical component of libpq's asynchronous query processing infrastructure
- **Error consolidation**: Ensures that internal libpq errors are properly converted to user-visible PGresult objects
- **Thread safety**: Used in conjunction with thread unlocking mechanisms (pgunlock_thread)