# pqTraceOutput_ErrorResponse

## Location
src/interfaces/libpq/fe-trace.c: 320 - 325

## Overview
A static wrapper function that specifically handles tracing of ErrorResponse messages in PostgreSQL's libpq protocol tracing.

## Definition
```c
static void
pqTraceOutput_ErrorResponse(FILE *f, const char *message, int *cursor, bool regress)
```

## Detailed Description
This function serves as a specialized wrapper around pqTraceOutputNR for handling ErrorResponse protocol messages. It delegates the actual tracing work to the shared pqTraceOutputNR function while providing the specific message type identifier "ErrorResponse". This design pattern allows for type-specific handling while leveraging common implementation code.

ErrorResponse messages are sent by the PostgreSQL server to indicate errors that have occurred during query processing or other operations. The tracing of these messages is crucial for debugging client-server communication issues.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: The raw ErrorResponse protocol message containing error fields
- `cursor`: Pointer to the current position in the message buffer, updated during processing
- `regress`: Boolean flag indicating regression test mode (enables field suppression for consistent output)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputNR](pqTraceOutputNR.md)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This function is static and only accessible within fe-trace.c
- ErrorResponse messages contain various fields like severity, SQLSTATE, message text, detail, hint, position, etc.
- The function maintains the same parameter interface as other message-specific tracing functions for consistency
- Part of PostgreSQL's libpq library which provides the C API for client applications