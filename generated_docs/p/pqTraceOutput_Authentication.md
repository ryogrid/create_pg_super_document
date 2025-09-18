# pqTraceOutput_Authentication

## Location
src/interfaces/libpq/fe-trace.c: 428 - 434

## Overview
Outputs a formatted trace message for Authentication protocol messages in PostgreSQL client tracing.

## Definition
```c
static void pqTraceOutput_Authentication(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure, specifically designed to format and output Authentication protocol messages. It writes an "Authentication" tab-delimited header followed by the authentication type code as a 32-bit integer to the trace output file. The function is used internally by the tracing system to provide human-readable logging of authentication exchanges between client and server.

## Parameters / Member Variables
- `f`: File pointer to the trace output destination
- `message`: Raw protocol message buffer containing the authentication data
- `cursor`: Pointer to current position in the message buffer (updated as data is read)

## Dependencies
- Functions called/Symbols referenced:
  - pqTraceOutputInt32
- Called from (representative examples):
  - pqTraceOutputMessage

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Part of the PostgreSQL frontend tracing system for debugging client-server protocol communication
- The function advances the cursor position through the message buffer as it reads the authentication type
- Authentication type codes correspond to various authentication methods (e.g., OK, MD5, GSSAPI, etc.)
- Output format follows tab-delimited structure for easy parsing by analysis tools