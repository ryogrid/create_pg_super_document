# pqTraceOutputInt32

## Location
[src/interfaces/libpq/fe-trace.c:144-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L144-L162)

## Overview
A static utility function that reads a 4-byte integer from a PostgreSQL protocol message buffer and outputs it to the trace log, with optional suppression of the actual value for security purposes.

## Definition
```c
static int pqTraceOutputInt32(FILE *pfdebug, const char *data, int *cursor, bool suppress)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure. It extracts a 4-byte integer from the protocol message data at the current cursor position, converts it from network byte order to host byte order, and writes it to the trace output file. The function includes a suppression mechanism that outputs 'NNNN' instead of the actual value when the suppress parameter is true, which is useful for hiding sensitive information like passwords or keys in trace logs.

The function advances the cursor by 4 bytes and returns the converted integer value regardless of whether it was suppressed in the output.

## Parameters / Member Variables
- `pfdebug`: FILE pointer to the trace output file where the integer value will be written
- `data`: Pointer to the message buffer containing the binary protocol data
- `cursor`: Pointer to the current position in the data buffer; updated by 4 bytes after reading
- `suppress`: Boolean flag indicating whether to output 'NNNN' instead of the actual integer value

## Dependencies
- Functions called/Symbols referenced:
  - pg_ntoh32 (network to host byte order conversion for 32-bit integers)
- Called from (representative examples):
  - [pqTraceOutput_NotificationResponse](pqTraceOutput_NotificationResponse.md)
  - [pqTraceOutput_Bind](pqTraceOutput_Bind.md)
  - [pqTraceOutput_DataRow](pqTraceOutput_DataRow.md)
  - [pqTraceOutput_Execute](pqTraceOutput_Execute.md)
  - [pqTraceOutput_FunctionCall](pqTraceOutput_FunctionCall.md)
  - [pqTraceOutput_BackendKeyData](pqTraceOutput_BackendKeyData.md)
  - [pqTraceOutput_Parse](pqTraceOutput_Parse.md)
  - [pqTraceOutput_Authentication](pqTraceOutput_Authentication.md)
  - [pqTraceOutput_ParameterDescription](pqTraceOutput_ParameterDescription.md)
  - [pqTraceOutput_RowDescription](pqTraceOutput_RowDescription.md)
  - [pqTraceOutput_NegotiateProtocolVersion](pqTraceOutput_NegotiateProtocolVersion.md)
  - [pqTraceOutput_FunctionCallResponse](pqTraceOutput_FunctionCallResponse.md)
  - [pqTraceOutputNoTypeByteMessage](pqTraceOutputNoTypeByteMessage.md)

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- The suppress feature is particularly important for security when tracing authentication-related messages
- Used extensively throughout the protocol tracing system for parsing 32-bit integer fields
- Always outputs the value prefixed with a space for consistent formatting
- The function modifies the cursor position as a side effect, enabling sequential parsing of protocol messages