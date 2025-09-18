# PQsendPrepare

## Location
src/interfaces/libpq/fe-exec.c: 1536 - 1632

## Overview
PQsendPrepare is a public API function that sends a Parse message to prepare a SQL statement on the PostgreSQL server for later execution, operating asynchronously without waiting for completion.

## Definition


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
  - PQsendQueryStart: Validates connection state and prepares for query sending
  - pqAllocCmdQueueEntry: Allocates a command queue entry for tracking the operation
  - pqPutMsgStart: Starts construction of a Parse protocol message
  - pqPuts: Writes string data to the output buffer
  - pqPutInt: Writes integer data to the output buffer
  - pqPutMsgEnd: Completes the protocol message construction
  - pqPipelineFlush: Flushes output buffer with pipeline mode awareness
  - pqAppendCmdQueueEntry: Adds the command queue entry to the connection's queue
  - pqRecycleCmdQueueEntry: Cleans up command queue entry on failure
- Called from (representative examples):
  - PQprepare: Synchronous version that waits for preparation to complete
  - test_pipelined_insert: Pipeline mode testing function
  - test_prepared: Prepared statement testing function
  - test_transaction: Transaction testing function

## Notes and Other Information
- Part of the extended query protocol's prepare-bind-execute sequence for optimal performance
- Supports both named and unnamed prepared statements (empty string for unnamed)
- Parameter type specification is optional - server can infer types if paramTypes is NULL
- Automatically handles protocol synchronization by adding Sync messages outside pipeline mode
- Pipeline mode compatible - omits Sync messages when in pipeline mode for better performance
- Query text is stored in the command queue entry for debugging and tracking purposes
- Enforces parameter limits to prevent resource exhaustion (PQ_QUERY_PARAM_MAX_LIMIT)
- Prepared statements persist for the duration of the database session unless explicitly deallocated
- Provides foundation for high-performance applications that execute the same queries repeatedly with different parameters