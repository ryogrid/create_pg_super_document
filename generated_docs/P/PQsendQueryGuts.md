# PQsendQueryGuts

## Location
[src/interfaces/libpq/fe-exec.c:1757-1924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1757-L1924)

## Overview
PQsendQueryGuts is a static function that implements the common code for sending a query using PostgreSQL's extended query protocol, handling the construction and transmission of Parse, Bind, Describe, Execute, and optionally Sync messages.

## Definition

```c
struct the Parse message */
		if (pqPutMsgStart(PqMsg_Parse, conn) < 0 ||
			pqPuts(stmtName, conn) < 0 ||
			pqPuts(command, conn) < 0)
			goto sendFailed;
```
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

## Simplified Source

```c
static int PQsendQueryGuts(PGconn *conn, const char *command, const char *stmtName,
                          int nParams, const Oid *paramTypes,
                          const char *const *paramValues, const int *paramLengths,
                          const int *paramFormats, int resultFormat) {
    PGcmdQueueEntry *entry = pqAllocCmdQueueEntry(conn);
    if (entry == NULL)
        return 0;

    // Send Parse message if command provided
    if (command) {
        if (pqPutMsgStart(PqMsg_Parse, conn) < 0 ||
            pqPuts(stmtName, conn) < 0 ||
            pqPuts(command, conn) < 0)
            goto sendFailed;

        // Send parameter types
        if (nParams > 0 && paramTypes) {
            if (pqPutInt(nParams, 2, conn) < 0)
                goto sendFailed;
            for (int i = 0; i < nParams; i++) {
                if (pqPutInt(paramTypes[i], 4, conn) < 0)
                    goto sendFailed;
            }
        } else {
            if (pqPutInt(0, 2, conn) < 0)
                goto sendFailed;
        }
        if (pqPutMsgEnd(conn) < 0)
            goto sendFailed;
    }

    // Send Bind message
    if (pqPutMsgStart(PqMsg_Bind, conn) < 0 ||
        pqPuts("", conn) < 0 ||
        pqPuts(stmtName, conn) < 0)
        goto sendFailed;

    // Send parameter formats and values
    if (nParams > 0 && paramFormats) {
        if (pqPutInt(nParams, 2, conn) < 0)
            goto sendFailed;
        for (int i = 0; i < nParams; i++) {
            if (pqPutInt(paramFormats[i], 2, conn) < 0)
                goto sendFailed;
        }
    } else {
        if (pqPutInt(0, 2, conn) < 0)
            goto sendFailed;
    }

    if (pqPutInt(nParams, 2, conn) < 0)
        goto sendFailed;

    // Send parameter values
    for (int i = 0; i < nParams; i++) {
        if (paramValues && paramValues[i]) {
            int nbytes = (paramFormats && paramFormats[i] != 0) ?
                        paramLengths[i] : strlen(paramValues[i]);
            if (pqPutInt(nbytes, 4, conn) < 0 ||
                pqPutnchar(paramValues[i], nbytes, conn) < 0)
                goto sendFailed;
        } else {
            if (pqPutInt(-1, 4, conn) < 0)
                goto sendFailed;
        }
    }

    // Send result format, Describe, Execute messages
    if (pqPutInt(1, 2, conn) < 0 ||
        pqPutInt(resultFormat, 2, conn) < 0 ||
        pqPutMsgEnd(conn) < 0)
        goto sendFailed;

    // Send Describe Portal, Execute, and optionally Sync
    if (pqPutMsgStart(PqMsg_Describe, conn) < 0 ||
        pqPutc('P', conn) < 0 ||
        pqPuts("", conn) < 0 ||
        pqPutMsgEnd(conn) < 0)
        goto sendFailed;

    if (pqPutMsgStart(PqMsg_Execute, conn) < 0 ||
        pqPuts("", conn) < 0 ||
        pqPutInt(0, 4, conn) < 0 ||
        pqPutMsgEnd(conn) < 0)
        goto sendFailed;

    if (conn->pipelineStatus == PQ_PIPELINE_OFF) {
        if (pqPutMsgStart(PqMsg_Sync, conn) < 0 ||
            pqPutMsgEnd(conn) < 0)
            goto sendFailed;
    }

    // Finalize and flush
    entry->queryclass = PGQUERY_EXTENDED;
    if (command)
        entry->query = strdup(command);

    if (pqPipelineFlush(conn) < 0)
        goto sendFailed;

    pqAppendCmdQueueEntry(conn, entry);
    return 1;

sendFailed:
    pqRecycleCmdQueueEntry(conn, entry);
    return 0;
}
```