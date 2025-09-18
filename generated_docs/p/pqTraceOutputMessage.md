# pqTraceOutputMessage

## Location
src/interfaces/libpq/fe-trace.c: 514 - 695

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