# PQsendQueryGuts

## Location
src/interfaces/libpq/fe-exec.c: 1757 - 1924

## Overview
PQsendQueryGuts is a static function that implements the common code for sending a query using PostgreSQL's extended query protocol, handling the construction and transmission of Parse, Bind, Describe, Execute, and optionally Sync messages.

## Definition


## Detailed Description
PQsendQueryGuts is the core implementation function for PostgreSQL's extended query protocol. It constructs and sends a sequence of protocol messages to execute a parameterized query or prepared statement. The function handles both cases where a new statement needs to be parsed (when command is provided) and when an already-prepared statement is executed (when command is NULL).

The function follows the extended query protocol sequence:
1. Parse message (if command is provided) - prepares the SQL statement
2. Bind message - binds parameters to the statement and specifies result format
3. Describe Portal message - requests information about the result columns
4. Execute message - executes the bound statement
5. Sync message (if not in pipeline mode) - requests synchronization

The function is designed to work with both regular query execution and pipeline mode, where multiple queries can be batched before synchronization.

## Parameters / Member Variables
- : The PostgreSQL connection handle
- : SQL command string to parse (may be NULL for prepared statements)
- : Name of the prepared statement to use
- : Number of parameters in the query
- : Array of parameter type OIDs (optional)
- : Array of parameter values as strings
- : Array of parameter lengths (required for binary parameters)
- : Array specifying text (0) or binary (1) format for each parameter
- : Format for result columns (0 for text, 1 for binary)

## Dependencies
- Functions called/Symbols referenced:
  - [pqAllocCmdQueueEntry](../p/pqAllocCmdQueueEntry.md)
  - [pqPutMsgStart](../p/pqPutMsgStart.md), pqPutMsgEnd
  - [pqPuts](../p/pqPuts.md), pqPutc, pqPutnchar, pqPutInt
  - [pqPipelineFlush](../p/pqPipelineFlush.md)
  - [pqAppendCmdQueueEntry](../p/pqAppendCmdQueueEntry.md), pqRecycleCmdQueueEntry
  - [PGcmdQueueEntry](PGcmdQueueEntry.md), PGQUERY_EXTENDED
  - Protocol message types: PqMsg_Parse, PqMsg_Bind, PqMsg_Describe, PqMsg_Execute, PqMsg_Sync
- Called from (representative examples):
  - [PQsendQueryParams](PQsendQueryParams.md)
  - [PQsendQueryPrepared](PQsendQueryPrepared.md)

## Notes and Other Information
- This is a static function used internally by libpq and not exposed to client applications
- The function assumes PQsendQueryStart has already been called to validate the connection state
- Supports both text and binary parameter formats, with automatic length calculation for text parameters
- Handles NULL parameters by sending -1 as the parameter length
- Uses the unnamed portal ("") for statement execution
- Pipeline mode optimization: only flushes data when past the size threshold
- Error handling uses goto sendFailed pattern for cleanup
- Memory allocation for query text copy uses strdup and gracefully handles allocation failure