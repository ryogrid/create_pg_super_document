# pqClearAsyncResult

## Location
src/interfaces/libpq/fe-exec.c: 779 - 802

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
  - PQclear (for freeing both conn->result and conn->saved_result)

- Called from (representative examples):
  - PQconnectPoll (during connection establishment)
  - pqClosePGconn (during connection cleanup)
  - pqSaveErrorResult (before saving error results)
  - PQsendQueryStart (when starting new queries)
  - pqPipelineProcessQueue (during pipeline processing)
  - getRowDescriptions (during result processing)
  - getParamDescriptions (during parameter description processing)
  - getAnotherTuple (during tuple fetching)
  - pqGetErrorNotice3 (during error/notice processing)

## Notes and Other Information
- **Internal function**: This is an internal libpq function, not part of the public API
- **State management**: Critical for maintaining clean state during asynchronous operations
- **Error handling**: Helps ensure that error conditions don't leave stale results in the connection
- **Memory safety**: Properly nullifies pointers after freeing to prevent dangling references
- **Asynchronous operations**: Specifically designed to support libpq's asynchronous query processing capabilities
- **Pipeline support**: Used in pipeline mode to maintain clean state between operations