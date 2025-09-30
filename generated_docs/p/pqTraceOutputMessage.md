# pqTraceOutputMessage

## Location
[src/interfaces/libpq/fe-trace.c:514-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L514-L695)

## Overview
Main dispatcher function for PostgreSQL's protocol message tracing system that parses and formats protocol messages between client and server for debugging purposes.

## Definition
```c
void pqTraceOutputMessage(PGconn *conn, const char *message, bool toServer)
```

## Detailed Description
This function serves as the central message dispatcher for PostgreSQL's libpq protocol tracing system. It receives raw protocol messages and routes them to appropriate specialized formatting functions based on the message type identifier. The function handles both frontend-to-backend (client-to-server) and backend-to-frontend (server-to-client) message tracing.

The function first extracts the message identifier byte and length field, then uses a large switch statement to dispatch to the appropriate message-specific formatting function. It supports various trace formatting modes including regression testing mode (which suppresses variable content) and timestamp suppression.

The function performs comprehensive message validation by comparing the actual bytes consumed during parsing with the expected message length, helping detect protocol parsing bugs and message corruption.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle containing trace configuration and output stream
- `message`: Pointer to the raw protocol message buffer starting with the message type identifier
- `toServer`: Boolean flag indicating message direction (true = client-to-server, false = server-to-client)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceFormatTimestamp](pqTraceFormatTimestamp.md) (formats timestamps for trace output)
  - pg_ntoh32 (network byte order conversion)
  - Multiple pqTraceOutput_* functions for specific message types:
    - [pqTraceOutput_NegotiateProtocolVersion](pqTraceOutput_NegotiateProtocolVersion.md)
    - [pqTraceOutput_FunctionCallResponse](pqTraceOutput_FunctionCallResponse.md)  
    - [pqTraceOutput_CopyBothResponse](pqTraceOutput_CopyBothResponse.md)
    - [pqTraceOutput_ReadyForQuery](pqTraceOutput_ReadyForQuery.md)
    - And many others for different protocol message types
  - fprintf (standard C library)
- Called from (representative examples):
  - [pqPutMsgEnd](pqPutMsgEnd.md) (when sending messages to server)
  - [pqParseInput3](pqParseInput3.md) (when receiving messages from server)
  - [getCopyDataMessage](../g/getCopyDataMessage.md) (during COPY operations)
  - [pqFunctionCall3](pqFunctionCall3.md) (during function call operations)

## Notes and Other Information
- This is a public function within libpq's tracing infrastructure
- Supports over 30 different PostgreSQL protocol message types
- Handles message identifier conflicts where frontend and backend use the same byte value for different message types
- Regression mode suppresses length information for ErrorResponse and NoticeResponse messages to ensure test stability
- Provides comprehensive error detection by validating message length consumption
- Essential for debugging PostgreSQL client-server communication issues
- The function outputs tab-separated format suitable for parsing by analysis tools
- CopyData messages are intentionally not fully traced to reduce logging overhead
- Message validation helps detect protocol implementation bugs and data corruption issues

## Simplified Source

```c
void pqTraceOutputMessage(PGconn *conn, const char *message, bool toServer) {
    char id;
    int length;
    char *prefix = toServer ? "F" : "B";
    int logCursor = 0;
    bool regress = (conn->traceFlags & PQTRACE_REGRESS_MODE) != 0;

    // Print timestamp if not suppressed
    if ((conn->traceFlags & PQTRACE_SUPPRESS_TIMESTAMPS) == 0) {
        char timestr[128];
        pqTraceFormatTimestamp(timestr, sizeof(timestr));
        fprintf(conn->Pfdebug, "%s\t", timestr);
    }

    // Extract message type and length
    id = message[logCursor++];
    memcpy(&length, message + logCursor, 4);
    length = (int) pg_ntoh32(length);
    logCursor += 4;

    // Print message header (suppress length for certain messages in regress mode)
    if (regress && !toServer && (id == PqMsg_ErrorResponse || id == PqMsg_NoticeResponse))
        fprintf(conn->Pfdebug, "%s\tNN\t", prefix);
    else
        fprintf(conn->Pfdebug, "%s\t%d\t", prefix, length);

    // Dispatch to appropriate message handler based on message type
    switch (id) {
        case PqMsg_ParseComplete:
        case PqMsg_BindComplete:
        case PqMsg_CloseComplete:
        case PqMsg_CopyDone:
        case PqMsg_EmptyQueryResponse:
        case PqMsg_NoData:
        case PqMsg_PortalSuspended:
        case PqMsg_Terminate:
            fprintf(conn->Pfdebug, "MessageName");  // Simple messages with no content
            break;

        case PqMsg_Query:
            pqTraceOutput_Query(conn->Pfdebug, message, &logCursor);
            break;

        case PqMsg_Parse:
            pqTraceOutput_Parse(conn->Pfdebug, message, &logCursor, regress);
            break;

        case PqMsg_Bind:
            pqTraceOutput_Bind(conn->Pfdebug, message, &logCursor);
            break;

        case PqMsg_Execute:
            // Handle shared identifier between Execute(F) and ErrorResponse(B)
            if (toServer)
                pqTraceOutput_Execute(conn->Pfdebug, message, &logCursor, regress);
            else
                pqTraceOutput_ErrorResponse(conn->Pfdebug, message, &logCursor, regress);
            break;

        case PqMsg_Describe:
            // Handle shared identifier between Describe(F) and DataRow(B)
            if (toServer)
                pqTraceOutput_Describe(conn->Pfdebug, message, &logCursor);
            else
                pqTraceOutput_DataRow(conn->Pfdebug, message, &logCursor);
            break;

        case PqMsg_Sync:
            // Handle shared identifier between Sync(F) and ParameterStatus(B)
            if (toServer)
                fprintf(conn->Pfdebug, "Sync");
            else
                pqTraceOutput_ParameterStatus(conn->Pfdebug, message, &logCursor);
            break;

        case PqMsg_CopyData:
            // Intentionally skip COPY data to reduce logging overhead
            break;

        // Additional message types handled by specialized functions
        case PqMsg_NotificationResponse:
        case PqMsg_CommandComplete:
        case PqMsg_AuthenticationRequest:
        case PqMsg_RowDescription:
        case PqMsg_ReadyForQuery:
            // Each calls its respective pqTraceOutput_* function
            break;

        default:
            fprintf(conn->Pfdebug, "Unknown message: %02x", id);
            break;
    }

    fputc('\n', conn->Pfdebug);

    // Validate message length consumption
    if (logCursor - 1 != length) {
        fprintf(conn->Pfdebug,
                "mismatched message length: consumed %d, expected %d\n",
                logCursor - 1, length);
    }
}
```