# pqTraceOutput_Close

## Location
src/interfaces/libpq/fe-trace.c: 258 - 265

## Overview
Outputs a formatted trace of a PostgreSQL Close message to a file stream, parsing and displaying the object type and name to be closed.

## Definition
```c
static void pqTraceOutput_Close(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing functionality and handles the parsing and output formatting of Close protocol messages. The Close message is used in the PostgreSQL extended query protocol to close a previously created prepared statement or portal. The function parses the message components in sequence:

1. Outputs the "Close" message type identifier
2. Extracts and displays the object type (single byte: 'S' for statement, 'P' for portal)
3. Extracts and displays the name of the object to be closed

This is a relatively simple message format compared to other protocol messages, containing only the object type indicator and the object name.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: Pointer to the raw protocol message buffer containing the Close message data
- `cursor`: Pointer to an integer tracking the current parsing position within the message buffer

## Dependencies
- Functions called/Symbols referenced:
  - pqTraceOutputByte1 (for the object type indicator)
  - pqTraceOutputString (for the object name)
- Called from (representative examples):
  - pqTraceOutputMessage (main message tracing dispatcher)

## Notes and Other Information
- This is a static function within fe-trace.c, making it internal to the libpq tracing implementation
- The object type byte typically contains 'S' to close a prepared statement or 'P' to close a portal
- The function assumes the message buffer contains a valid Close message and does not perform extensive error checking
- Part of PostgreSQL's debugging and development tools for analyzing client-server protocol communication
- The Close message is part of the extended query protocol and helps manage resource cleanup on the server side