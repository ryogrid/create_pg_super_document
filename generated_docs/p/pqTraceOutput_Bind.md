# pqTraceOutput_Bind

## Location
src/interfaces/libpq/fe-trace.c: 228 - 257

## Overview
Outputs a formatted trace of a PostgreSQL Bind message to a file stream, parsing and displaying the protocol components including parameter format codes, parameter values, and result format codes.

## Definition
```c
static void pqTraceOutput_Bind(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing functionality and specifically handles the parsing and output formatting of Bind protocol messages. The Bind message is used in the PostgreSQL extended query protocol to bind parameter values to a prepared statement. The function sequentially parses the message components:

1. Outputs the "Bind" message type identifier
2. Extracts and displays the destination portal name
3. Extracts and displays the source prepared statement name  
4. Parses parameter format codes (number and individual codes)
5. Parses parameter values (number, lengths, and actual data)
6. Parses result format codes (number and individual codes)

The function follows the PostgreSQL frontend/backend protocol specification for Bind messages, ensuring proper parsing of the binary message format.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: Pointer to the raw protocol message buffer containing the Bind message data
- `cursor`: Pointer to an integer tracking the current parsing position within the message buffer

## Dependencies
- Functions called/Symbols referenced:
  - pqTraceOutputString (for portal and statement names)
  - pqTraceOutputInt16 (for format codes and parameter counts)
  - pqTraceOutputInt32 (for parameter lengths)
  - pqTraceOutputNchar (for parameter data)
- Called from (representative examples):
  - pqTraceOutputMessage (main message tracing dispatcher)

## Notes and Other Information
- This is a static function within fe-trace.c, making it internal to the libpq tracing implementation
- The function assumes the message buffer contains a valid Bind message and does not perform extensive error checking
- Parameter values of length -1 are treated as NULL values and are skipped during output
- The function maintains the cursor position to enable sequential parsing of the message components
- Part of PostgreSQL's debugging and development tools for analyzing client-server protocol communication