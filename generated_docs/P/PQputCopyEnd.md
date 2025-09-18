# PQputCopyEnd

## Location
src/interfaces/libpq/fe-exec.c: 2749 - 2815

## Overview
Signals the end of a COPY IN operation to the PostgreSQL backend, either indicating successful completion or reporting an error condition.

## Definition


## Detailed Description
PQputCopyEnd terminates a COPY IN or COPY BOTH operation by sending either a COPY DONE message (successful completion) or a COPY FAIL message (error condition) to the PostgreSQL server. The function handles protocol-level details including proper message formatting and connection state transitions.

Key operations performed:
- Validates that a COPY operation is currently active
- Sends appropriate termination message based on error status
- Handles extended-query mode by sending Sync message when necessary
- Transitions connection state appropriately (COPY_BOTH to COPY_OUT, or to BUSY)
- Flushes the output buffer to ensure message delivery

The function supports both successful completion (errormsg = NULL) and error reporting (errormsg contains error description). After calling this function, the client should use PQgetResult() to check the final command completion status.

## Parameters / Member Variables
- : PostgreSQL connection handle that must be in COPY IN or COPY BOTH state
- : Error message string for failure cases, or NULL for successful completion

## Dependencies
- Functions called/Symbols referenced:
  - libpq_append_conn_error
  - pqPutMsgStart
  - pqPuts
  - pqPutMsgEnd
  - pqFlush
  - PqMsg_CopyFail
  - PqMsg_CopyDone
  - PqMsg_Sync
  - PGASYNC_COPY_IN
  - PGASYNC_COPY_BOTH
  - PGASYNC_COPY_OUT
  - PGASYNC_BUSY
  - PGQUERY_SIMPLE
- Called from (representative examples):
  - handleCopyIn (psql)
  - EndDBCopyMode (pg_dump)
  - libpqrcv_endstreaming (replication)
  - BaseBackup (pg_basebackup)

## Notes and Other Information
- Returns 1 on success or -1 on error
- Must be called to properly terminate any COPY IN operation
- In COPY BOTH mode, transitions connection to COPY OUT state for reading remaining data
- Automatically sends Sync message in extended-query mode for proper protocol compliance
- The errormsg parameter allows detailed error reporting to the server
- After successful completion, use PQgetResult() to get the final command status
- Critical for proper cleanup of COPY operations and connection state management