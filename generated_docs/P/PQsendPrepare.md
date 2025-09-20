# PQsendPrepare

## Location
[src/interfaces/libpq/fe-exec.c:1536-1632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1536-L1632)

## Overview
PQsendPrepare is a public API function that sends a Parse message to prepare a SQL statement on the PostgreSQL server for later execution, operating asynchronously without waiting for completion.

## Definition

```c
struct the Parse message */
	if (pqPutMsgStart(PqMsg_Parse, conn) < 0 ||
		pqPuts(stmtName, conn) < 0 ||
		pqPuts(query, conn) < 0)
		goto sendFailed;
```
## Detailed Description
PQsendPrepare implements the asynchronous preparation of SQL statements using PostgreSQL's extended query protocol. It sends a Parse message to the server, which parses and analyzes the SQL statement, creating a prepared statement that can be executed multiple times with different parameters. This function is part of the prepare-bind-execute cycle that provides improved performance for repeatedly executed queries.

The function constructs and sends a Parse message containing the statement name, query text, and optional parameter type information. It handles both pipeline and non-pipeline modes, automatically adding a Sync message when not in pipeline mode to ensure proper protocol synchronization. The prepared statement is stored on the server and referenced by its name for subsequent execution.

## Parameters / Member Variables
- : PostgreSQL connection handle for the database connection
- : Name to assign to the prepared statement (can be empty string for unnamed statement)
- : SQL statement text to be prepared, possibly containing parameter placeholders (, , etc.)
- : Number of parameters expected by the query
- : Array of PostgreSQL type OIDs for each parameter (can be NULL for automatic type inference)

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQueryStart](PQsendQueryStart.md): Validates connection state and prepares for query sending
  - [pqAllocCmdQueueEntry](../p/pqAllocCmdQueueEntry.md): Allocates a command queue entry for tracking the operation
  - [pqPutMsgStart](../p/pqPutMsgStart.md): Starts construction of a Parse protocol message
  - [pqPuts](../p/pqPuts.md): Writes string data to the output buffer
  - [pqPutInt](../p/pqPutInt.md): Writes integer data to the output buffer
  - [pqPutMsgEnd](../p/pqPutMsgEnd.md): Completes the protocol message construction
  - [pqPipelineFlush](../p/pqPipelineFlush.md): Flushes output buffer with pipeline mode awareness
  - [pqAppendCmdQueueEntry](../p/pqAppendCmdQueueEntry.md): Adds the command queue entry to the connection's queue
  - [pqRecycleCmdQueueEntry](../p/pqRecycleCmdQueueEntry.md): Cleans up command queue entry on failure
- Called from (representative examples):
  - [PQprepare](PQprepare.md): Synchronous version that waits for preparation to complete
  - [test_pipelined_insert](../t/test_pipelined_insert.md): Pipeline mode testing function
  - [test_prepared](../t/test_prepared.md): Prepared statement testing function
  - [test_transaction](../t/test_transaction.md): Transaction testing function

## Notes and Other Information
- Part of the extended query protocol's prepare-bind-execute sequence for optimal performance
- Supports both named and unnamed prepared statements (empty string for unnamed)
- Parameter type specification is optional - server can infer types if paramTypes is NULL
- Automatically handles protocol synchronization by adding Sync messages outside pipeline mode
- Pipeline mode compatible - omits Sync messages when in pipeline mode for better performance
- [Query](../Q/Query.md) text is stored in the command queue entry for debugging and tracking purposes
- Enforces parameter limits to prevent resource exhaustion (PQ_QUERY_PARAM_MAX_LIMIT)
- Prepared statements persist for the duration of the database session unless explicitly deallocated
- Provides foundation for high-performance applications that execute the same queries repeatedly with different parameters