# pqTraceOutput_NoticeResponse

## Location
[src/interfaces/libpq/fe-trace.c:326-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L326-L331)

## Overview
A static wrapper function that specifically handles tracing of NoticeResponse messages in PostgreSQL's libpq protocol tracing.

## Definition
```c
static void
pqTraceOutput_NoticeResponse(FILE *f, const char *message, int *cursor, bool regress)
```

## Detailed Description
This function serves as a specialized wrapper around pqTraceOutputNR for handling NoticeResponse protocol messages. Like its ErrorResponse counterpart, it delegates the actual tracing work to the shared pqTraceOutputNR function while providing the specific message type identifier "NoticeResponse". This maintains consistency in the tracing architecture while allowing for message-type-specific processing.

NoticeResponse messages are sent by the PostgreSQL server to provide informational messages, warnings, or other non-error notifications to client applications. These messages have the same field structure as ErrorResponse messages but indicate conditions that don't prevent query execution from continuing.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: The raw NoticeResponse protocol message containing notification fields
- `cursor`: Pointer to the current position in the message buffer, updated during processing
- `regress`: Boolean flag indicating regression test mode (enables field suppression for consistent output)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputNR](pqTraceOutputNR.md)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This function is static and only accessible within fe-trace.c
- NoticeResponse messages share the same field structure as ErrorResponse messages (severity, code, message, etc.)
- Common use cases include warnings about deprecated features, informational messages about query planning, or notices about database configuration
- The function maintains parameter consistency with other message-specific tracing functions
- Part of the libpq tracing infrastructure used for debugging client-server protocol communication