# pqParseInput3

## Location
src/interfaces/libpq/fe-protocol3.c: 66 - 482

## Overview
pqParseInput3 is the main message parsing function for PostgreSQL's protocol version 3, responsible for processing all incoming messages from the backend server until input is exhausted or a stopping state is reached.

## Definition


## Detailed Description
This function implements the core message processing loop for the PostgreSQL client-server protocol version 3. It continuously reads and processes complete messages from the input buffer, handling various message types including query results, notifications, errors, and protocol control messages. The function validates message headers, manages connection state transitions, and dispatches messages to appropriate handlers based on the current connection state (IDLE, BUSY, COPY modes, etc.).

The function operates in a stateful manner, respecting the connection's async status and handling special cases like NOTIFY/NOTICE messages that can arrive at any time, versus other messages that should only be processed during specific states. It includes robust error handling for malformed messages and implements protocol validation to detect synchronization loss.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection, containing input buffer, connection state, and result information

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetc](pqGetc.md) (reads single character from input buffer)
  - [pqGetInt](pqGetInt.md) (reads integer values from input buffer)
  - [handleSyncLoss](../h/handleSyncLoss.md) (handles protocol synchronization errors)
  - VALID_LONG_MESSAGE_TYPE (macro validating message types for large messages)
  - [pqCheckInBufferSpace](pqCheckInBufferSpace.md) (ensures sufficient buffer space)
  - [getNotify](../g/getNotify.md) (processes notification messages)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md) (processes error and notice messages)
  - [getParameterStatus](../g/getParameterStatus.md) (processes parameter status messages)
  - [getReadyForQuery](../g/getReadyForQuery.md) (processes ready-for-query messages)
  - [getRowDescriptions](../g/getRowDescriptions.md) (processes row description messages)
  - [getParamDescriptions](../g/getParamDescriptions.md) (processes parameter description messages)
  - [getAnotherTuple](../g/getAnotherTuple.md) (processes data row messages)
  - [getCopyStart](../g/getCopyStart.md) (initiates COPY operations)
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (debug tracing)
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md) (creates empty result objects)
  - [pqSaveErrorResult](pqSaveErrorResult.md) (saves error results)
  - pqCommandQueueAdvance (advances command queue)
- Called from (representative examples):
  - [parseInput](parseInput.md) (from src/interfaces/libpq/fe-exec.c:2022)
  - pgunlock_thread (from src/interfaces/libpq/libpq-int.h:720)

## Notes and Other Information
- The function does NOT attempt to read more data from the backend; it only processes what's already in the input buffer
- Implements comprehensive message type handling for protocol version 3 including all standard PostgreSQL message types
- Maintains strict state machine behavior, ensuring messages are processed only in appropriate connection states
- Includes sophisticated error recovery mechanisms for malformed messages and buffer management
- Critical for libpq's asynchronous operation model, allowing non-blocking query processing
- The parsing loop continues until either the input buffer is exhausted or a state change requires stopping