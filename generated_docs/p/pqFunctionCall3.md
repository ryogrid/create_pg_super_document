# pqFunctionCall3

## Location
[src/interfaces/libpq/fe-protocol3.c:2009-2236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L2009-L2236)

## Overview
Executes PostgreSQL server-side function calls using protocol 3, handling the complete message exchange including parameter serialization, response processing, and error handling.

## Definition


## Detailed Description
pqFunctionCall3 implements the protocol 3 function call mechanism for PostgreSQL, enabling clients to execute server-side functions directly. It constructs and sends a FunctionCall message with the function OID and serialized arguments, then processes the server response. The function handles both integer and binary data arguments, manages message framing, validates message integrity, and processes various response types including function results, errors, notices, and notifications.

The function uses a state machine approach to process incoming messages, handling partial reads and ensuring proper synchronization with the server protocol.

## Parameters / Member Variables
- : PostgreSQL connection handle for the database session
- : Object ID (OID) of the server-side function to execute
- : Buffer to store the function's return value
- : Pointer to store the actual length of the returned result
- : Flag indicating whether the result should be treated as an integer
- : Array of PQArgBlock structures containing function arguments
- : Number of arguments in the args array

## Dependencies
- Functions called/Symbols referenced:
  - [pqPutMsgStart](pqPutMsgStart.md), pqPutMsgEnd
  - [pqPutInt](pqPutInt.md), pqPutnchar
  - [pqFlush](pqFlush.md), pqWait, pqReadData
  - [pqGetc](pqGetc.md), pqGetInt, pqGetnchar
  - [handleSyncLoss](../h/handleSyncLoss.md), pqCheckInBufferSpace
  - [pqGetErrorNotice3](pqGetErrorNotice3.md), getNotify, getReadyForQuery, getParameterStatus
  - pgHavePendingResult, PQmakeEmptyPGresult
  - [pqSaveErrorResult](pqSaveErrorResult.md), pqPrepareAsyncResult
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)
  - PqMsg_FunctionCall (message type)
  - PGRES_COMMAND_OK, PGRES_FATAL_ERROR (result status constants)
  - PQ_PIPELINE_OFF (pipeline status)
  - VALID_LONG_MESSAGE_TYPE (message validation macro)
- Called from (representative examples):
  - PQfn (in src/interfaces/libpq/fe-exec.c)

## Notes and Other Information
- Returns a PGresult pointer containing the function result or error information
- Supports both integer and binary argument types through the PQArgBlock structure
- Handles NULL arguments by setting len to -1 in the argument structure
- Uses binary format for both input arguments and output results
- Implements comprehensive message validation including length checks and type validation
- Processes various message types: 'V' (function result), 'E' (error), 'A' (notify), 'N' (notice), 'Z' (ready for query), 'S' (parameter status)
- Maintains protocol synchronization and handles partial message reads
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication
- Includes debug tracing support when conn->Pfdebug is enabled