# pqTraceOutput_NegotiateProtocolVersion

## Location
src/interfaces/libpq/fe-trace.c: 475 - 482

## Overview
Outputs a formatted trace message for PostgreSQL's NegotiateProtocolVersion backend message, displaying the protocol version numbers in a readable format for debugging purposes.

## Definition
```c
static void pqTraceOutput_NegotiateProtocolVersion(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq client library tracing system. It specifically handles the parsing and output formatting of NegotiateProtocolVersion messages received from the PostgreSQL backend. The function extracts two 32-bit integers from the message buffer and displays them as part of the trace output. NegotiateProtocolVersion messages are sent by the server when it needs to negotiate a lower protocol version with the client due to compatibility requirements.

The function reads two consecutive 32-bit integers from the message buffer using pqTraceOutputInt32(), which handles network byte order conversion and cursor advancement automatically.

## Parameters / Member Variables
- `f`: FILE pointer to the trace output destination (typically stderr or a log file)
- `message`: Pointer to the message buffer containing the raw protocol message data
- `cursor`: Pointer to an integer tracking the current read position within the message buffer; updated as data is consumed

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard C library)
  - [pqTraceOutputInt32](pqTraceOutputInt32.md) (reads and formats 32-bit integers from message buffer)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message dispatcher for trace output)

## Notes and Other Information
- This is a static function within fe-trace.c, part of the internal tracing infrastructure
- The function outputs "NegotiateProtocolVersion" as a tab-separated label followed by two integer values
- The two integers typically represent the supported protocol version numbers that the server is proposing
- The function assumes the message buffer contains at least 8 bytes of data (two 32-bit integers) starting at the cursor position
- Part of PostgreSQL's protocol-level debugging and diagnostics system for troubleshooting client-server communication issues