# pqTraceOutput_ParameterStatus

## Location
src/interfaces/libpq/fe-trace.c: 435 - 442

## Overview
Outputs a formatted trace message for ParameterStatus protocol messages in PostgreSQL client tracing.

## Definition
```c
static void pqTraceOutput_ParameterStatus(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure, specifically designed to format and output ParameterStatus protocol messages. It writes a "ParameterStatus" tab-delimited header followed by two string values: the parameter name and its value. ParameterStatus messages are sent by the server to inform the client about runtime parameter changes or initial parameter values during connection establishment.

## Parameters / Member Variables
- `f`: File pointer to the trace output destination
- `message`: Raw protocol message buffer containing the parameter status data
- `cursor`: Pointer to current position in the message buffer (updated as data is read)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputString](pqTraceOutputString.md) (called twice)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Part of the PostgreSQL frontend tracing system for debugging client-server protocol communication
- The function reads two consecutive null-terminated strings from the message buffer
- First string is the parameter name, second string is the parameter value
- Common parameters include server_version, server_encoding, client_encoding, etc.
- Output format follows tab-delimited structure for easy parsing by analysis tools
- The cursor is advanced twice as both parameter name and value are read from the message