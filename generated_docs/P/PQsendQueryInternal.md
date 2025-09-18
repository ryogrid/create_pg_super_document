# PQsendQueryInternal

## Location
[src/interfaces/libpq/fe-exec.c:1428-1491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1428-L1491)

## Overview
PQsendQueryInternal is a static function that implements the core logic for sending a simple SQL query using PostgreSQL's simple query protocol in asynchronous mode.

## Definition


## Detailed Description
PQsendQueryInternal handles the internal mechanics of sending a SQL query string to a PostgreSQL server using the simple query protocol. This function performs validation checks, constructs the Query message according to the PostgreSQL wire protocol, and manages the command queue entry for tracking the query's lifecycle. The function operates asynchronously, meaning it sends the query without waiting for a response.

The function ensures proper pipeline mode handling by rejecting queries when pipeline mode is active (since simple queries are not allowed in pipeline mode). It allocates a command queue entry to track the query, constructs and sends the Query message, and manages error conditions by properly cleaning up allocated resources.

## Parameters / Member Variables
- : PostgreSQL connection handle that represents the database connection
- : SQL query string to be executed on the server
- : Boolean flag indicating whether this is a new query (affects connection state validation)

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQueryStart](PQsendQueryStart.md): Validates connection state and prepares for query sending
  - [pqAllocCmdQueueEntry](../p/pqAllocCmdQueueEntry.md): Allocates a new command queue entry for tracking the query
  - [pqPutMsgStart](../p/pqPutMsgStart.md): Starts construction of a PostgreSQL protocol message
  - [pqPuts](../p/pqPuts.md): Writes a null-terminated string to the output buffer
  - [pqPutMsgEnd](../p/pqPutMsgEnd.md): Completes the protocol message construction
  - [pqFlush](../p/pqFlush.md): Attempts to flush the output buffer to the network
  - [pqAppendCmdQueueEntry](../p/pqAppendCmdQueueEntry.md): Adds the command queue entry to the connection's queue
  - [pqRecycleCmdQueueEntry](../p/pqRecycleCmdQueueEntry.md): Cleans up and deallocates a command queue entry
- Called from (representative examples):
  - [PQsendQuery](PQsendQuery.md): Public API function for sending simple queries
  - [PQsendQueryContinue](PQsendQueryContinue.md): Function for continuing query sending operations

## Notes and Other Information
- This function is part of the simple query protocol implementation, distinct from the extended query protocol used by prepared statements
- Pipeline mode restrictions are enforced - simple queries cannot be sent while in pipeline mode
- The function performs proper resource management by cleaning up command queue entries on failure
- [Query](../Q/Query.md) text is duplicated and stored in the command queue entry for debugging and tracking purposes
- The function supports both blocking and non-blocking operation modes through the underlying flush mechanism
- Error handling follows libpq conventions with error messages appended to the connection's error buffer