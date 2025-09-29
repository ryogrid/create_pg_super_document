# pqClearAsyncResult

## Location
[src/interfaces/libpq/fe-exec.c:779-802](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L779-L802)

## Overview
pqClearAsyncResult is an internal libpq utility function that deallocates any partially constructed asynchronous result and clears both current and saved result structures in a connection.

## Definition
```c
void pqClearAsyncResult(PGconn *conn)
```

## Detailed Description
pqClearAsyncResult provides comprehensive cleanup of result-related state in a PGconn structure during asynchronous operations. The function performs the following cleanup operations:

1. **Current result cleanup**: Calls PQclear() on the current result and sets the pointer to NULL
2. **Error state reset**: Clears the error_result flag to indicate no error result is pending
3. **Saved result cleanup**: Calls PQclear() on any saved result and sets that pointer to NULL

This function is essential for proper state management during asynchronous query processing, ensuring that partially constructed or stale results don't interfere with new operations. It's commonly called when starting new queries, handling connection errors, or resetting connection state.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure whose async result state should be cleared

## Dependencies
- Functions called/Symbols referenced:
  - [PQclear](../P/PQclear.md) (for freeing both conn->result and conn->saved_result)

- Called from (representative examples):
  - [PQconnectPoll](../P/PQconnectPoll.md) (during connection establishment)
  - [pqClosePGconn](pqClosePGconn.md) (during connection cleanup)
  - [pqSaveErrorResult](pqSaveErrorResult.md) (before saving error results)
  - [PQsendQueryStart](../P/PQsendQueryStart.md) (when starting new queries)
  - [pqPipelineProcessQueue](pqPipelineProcessQueue.md) (during pipeline processing)
  - [getRowDescriptions](../g/getRowDescriptions.md) (during result processing)
  - [getParamDescriptions](../g/getParamDescriptions.md) (during parameter description processing)
  - [getAnotherTuple](../g/getAnotherTuple.md) (during tuple fetching)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md) (during error/notice processing)

## Notes and Other Information
- **Internal function**: This is an internal libpq function, not part of the public API
- **State management**: Critical for maintaining clean state during asynchronous operations
- **Error handling**: Helps ensure that error conditions don't leave stale results in the connection
- **Memory safety**: Properly nullifies pointers after freeing to prevent dangling references
- **Asynchronous operations**: Specifically designed to support libpq's asynchronous query processing capabilities
- **Pipeline support**: Used in pipeline mode to maintain clean state between operations

## Simplified Source

```c
void
pqClearAsyncResult(PGconn *conn)
{
    // Clear current result
    PQclear(conn->result);
    conn->result = NULL;
    conn->error_result = false;

    // Clear saved result
    PQclear(conn->saved_result);
    conn->saved_result = NULL;
}
```