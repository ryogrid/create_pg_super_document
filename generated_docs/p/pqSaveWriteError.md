# pqSaveWriteError

## Location
src/interfaces/libpq/fe-exec.c: 820 - 850

## Overview
pqSaveWriteError is an internal libpq function that reports write failures by appending write error messages to the connection's error buffer and marking the connection as having an error condition.

## Definition
```c
static void pqSaveWriteError(PGconn *conn)
```

## Detailed Description
pqSaveWriteError handles the reporting of write failures when communication with the PostgreSQL server fails. The function is designed to be resilient against out-of-memory conditions and provides comprehensive error reporting. It performs the following operations:

1. **Write error message handling**: If a write error message exists in conn->write_err_msg, it appends this message to the connection's error buffer
2. **Message deduplication**: Clears the write_err_msg buffer to prevent the same error from being appended multiple times
3. **Fallback error message**: If no specific write error message is available (possibly due to memory allocation failure), it provides a generic "write to server failed" message
4. **Error state marking**: Calls pqSaveErrorResult() to mark the connection as having an error condition

This function is typically called when write operations have failed and libpq has exhausted all attempts to recover or report alternative errors.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure where the write error should be recorded

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBufferStr (to append error messages to the connection's error buffer)
  - libpq_append_conn_error (to append generic error messages)
  - pqSaveErrorResult (to mark the connection as having an error condition)

- Called from (representative examples):
  - PQgetResult (when handling write errors during result retrieval)

## Notes and Other Information
- **Internal function**: This is a static internal libpq function, not part of the public API
- **Memory resilience**: Designed to handle out-of-memory conditions gracefully with fallback error messages
- **Error aggregation**: Appends write errors to existing error messages rather than replacing them
- **Message deduplication**: Prevents the same write error from being reported multiple times
- **Write failure handling**: Specifically designed for scenarios where network write operations to the PostgreSQL server have failed
- **Error propagation**: Uses the standard libpq error propagation mechanism through pqSaveErrorResult()
- **Fallback messaging**: Provides meaningful error messages even when memory allocation for specific error details fails