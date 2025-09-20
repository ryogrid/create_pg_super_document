# pqGetErrorNotice3

## Location
[src/interfaces/libpq/fe-protocol3.c:882-1013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L882-L1013)

## Overview
Processes Error or Notice response messages from the PostgreSQL server in protocol version 3, handling both error conditions and informational notices.

## Definition

```c
int
pqGetErrorNotice3(PGconn *conn, bool isError)
```
## Detailed Description
This function reads and processes Error ('E') or Notice ('N') messages from the PostgreSQL server using protocol version 3. The message type and length have already been consumed before this function is called. It creates a PGresult structure to hold the error/notice fields, builds a formatted error message, and either stores it as an async result (for errors) or processes it as a notice callback.

The function handles pipeline mode by setting the pipeline status to aborted when an error occurs. For errors, it pre-emptively clears any incomplete query results to avoid memory issues. It reads all message fields in a loop until a null terminator is found, saving special fields like SQLSTATE and statement position for later use.

## Parameters / Member Variables
- : PostgreSQL connection object containing connection state and buffers
- : Boolean flag indicating whether this is an Error (true) or Notice (false) message

## Dependencies
- Functions called/Symbols referenced:
  - [pqClearAsyncResult](pqClearAsyncResult.md)
  - initPQExpBuffer
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md)
  - [pqGetc](pqGetc.md)
  - [pqGets](pqGets.md)
  - [pqSaveMessageField](pqSaveMessageField.md)
  - strlcpy
  - [pqResultStrdup](pqResultStrdup.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [pqBuildErrorMessage3](pqBuildErrorMessage3.md)
  - [pqSetResultError](pqSetResultError.md)
  - PQExpBufferDataBroken
  - [libpq_gettext](../l/libpq_gettext.md)
  - termPQExpBuffer
- Called from (representative examples):
  - [pqParseInput3](pqParseInput3.md)
  - [getCopyDataMessage](../g/getCopyDataMessage.md)
  - [pqFunctionCall3](pqFunctionCall3.md)

## Notes and Other Information
- Returns 0 on successful message consumption, EOF if insufficient data available
- Uses a temporary PQExpBuffer instead of conn->workBuffer for potentially long field data
- Saves SQLSTATE in conn->last_sqlstate for error tracking
- Handles memory allocation failures gracefully by falling back to internal error processing
- For errors in pipeline mode, sets pipeline status to PQ_PIPELINE_ABORTED
- Notice messages trigger registered notice callbacks if available
- [Query](../Q/Query.md) text is preserved in the result only when statement position information is present