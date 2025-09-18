# pqTraceOutput_CopyFail

## Location
[src/interfaces/libpq/fe-trace.c:340-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L340-L346)

## Overview
A static function that handles tracing of CopyFail messages in PostgreSQL's libpq protocol tracing.

## Definition
```c
static void
pqTraceOutput_CopyFail(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function traces CopyFail protocol messages, which are sent by the client to indicate that a COPY FROM STDIN operation has failed and should be aborted. The CopyFail message contains an error message explaining why the copy operation failed. This is part of PostgreSQL's COPY protocol, which allows efficient bulk data transfer between the client and server.

When a client is sending data via COPY FROM STDIN and encounters an error or needs to abort the operation, it sends a CopyFail message to inform the server to terminate the copy operation gracefully.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: The raw CopyFail protocol message containing the error message
- `cursor`: Pointer to the current position in the message buffer, updated as the error message is processed

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard C library)
  - [pqTraceOutputString](pqTraceOutputString.md)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This function is static and only accessible within fe-trace.c
- The CopyFail message format consists of: error message (string)
- Unlike other tracing functions, this one doesn't have a regress parameter since CopyFail messages don't contain variable fields that need suppression during regression testing
- Part of PostgreSQL's COPY protocol which enables high-performance bulk data operations
- CopyFail messages are sent by the client, not the server, when aborting a COPY FROM STDIN operation
- The server responds to CopyFail with an ErrorResponse message to acknowledge the failed operation