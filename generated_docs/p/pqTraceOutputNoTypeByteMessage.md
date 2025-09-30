# pqTraceOutputNoTypeByteMessage

## Location
[src/interfaces/libpq/fe-trace.c:696-731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L696-L731)

## Overview
Outputs special PostgreSQL protocol messages that contain no type byte to the trace output stream for debugging purposes.

## Definition

```c
void
pqTraceOutputNoTypeByteMessage(PGconn *conn, const char *message)
```
## Detailed Description
This function is responsible for tracing and outputting special PostgreSQL frontend-backend protocol messages that do not contain a type byte. These are typically administrative messages like CancelRequest that have a fixed format and are identified by their length rather than a type byte. The function parses the message length from the first 4 bytes, formats it with optional timestamps, and outputs human-readable information about the message type and contents to the trace stream.

The function handles different message types based on their length:
- Length 16: CancelRequest messages (outputs three 32-bit integers)
- Length 8: GSSENCRequest or SSLRequest (though these don't typically reach this function)
- Other lengths: Unknown message types

## Parameters / Member Variables
- : PostgreSQL connection object containing trace configuration and output stream
- : Raw message buffer containing the protocol message to be traced

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceFormatTimestamp](pqTraceFormatTimestamp.md)
  - pg_ntoh32
  - [pqTraceOutputInt32](pqTraceOutputInt32.md)
- Constants referenced:
  - PQTRACE_SUPPRESS_TIMESTAMPS
- Called from (representative examples):
  - [pqPutMsgEnd](pqPutMsgEnd.md)
  - pgunlock_thread

## Notes and Other Information
- The function is part of PostgreSQL's libpq tracing infrastructure located in fe-trace.c
- Timestamps can be suppressed using the PQTRACE_SUPPRESS_TIMESTAMPS flag
- The function specifically handles messages without type bytes, which are special cases in the PostgreSQL protocol
- Network byte order conversion is performed using pg_ntoh32() for proper message length interpretation
- Output is directed to conn->Pfdebug file stream

## Simplified Source

```c
void pqTraceOutputNoTypeByteMessage(PGconn *conn, const char *message) {
    int length;
    int logCursor = 0;

    // Print timestamp if not suppressed
    if ((conn->traceFlags & PQTRACE_SUPPRESS_TIMESTAMPS) == 0) {
        char timestr[128];
        pqTraceFormatTimestamp(timestr, sizeof(timestr));
        fprintf(conn->Pfdebug, "%s\t", timestr);
    }

    // Extract message length (no type byte for these special messages)
    memcpy(&length, message + logCursor, 4);
    length = (int) pg_ntoh32(length);
    logCursor += 4;

    // Print message header with frontend prefix and length
    fprintf(conn->Pfdebug, "F\t%d\t", length);

    // Identify message type by length
    switch (length) {
        case 16:  // CancelRequest message
            fprintf(conn->Pfdebug, "CancelRequest\t");
            // Output the three 32-bit integers in CancelRequest
            pqTraceOutputInt32(conn->Pfdebug, message, &logCursor, false);  // Cancel code
            pqTraceOutputInt32(conn->Pfdebug, message, &logCursor, false);  // Process ID
            pqTraceOutputInt32(conn->Pfdebug, message, &logCursor, false);  // Secret key
            break;

        case 8:   // GSSENCRequest or SSLRequest (usually don't reach here)
        default:  // Unknown message type
            fprintf(conn->Pfdebug, "Unknown message: length is %d", length);
            break;
    }

    fputc('\n', conn->Pfdebug);
}
```