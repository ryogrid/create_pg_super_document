# pqTraceOutput_RowDescription

## Location
src/interfaces/libpq/fe-trace.c: 455 - 474

## Overview
Outputs a formatted trace message for RowDescription protocol messages in PostgreSQL client tracing.

## Definition
```c
static void pqTraceOutput_RowDescription(FILE *f, const char *message, int *cursor, bool regress)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure, specifically designed to format and output RowDescription protocol messages. It writes a "RowDescription" tab-delimited header followed by detailed metadata about each column in a result set. RowDescription messages are sent by the server before sending actual row data, providing clients with comprehensive information about the structure and types of the returned data.

## Parameters / Member Variables
- `f`: File pointer to the trace output destination
- `message`: Raw protocol message buffer containing the row description data
- `cursor`: Pointer to current position in the message buffer (updated as data is read)
- `regress`: Boolean flag indicating whether to use regression-friendly output format (affects OID and table OID display)

## Dependencies
- Functions called/Symbols referenced:
  - pqTraceOutputInt16 (called 4 times)
  - pqTraceOutputString
  - pqTraceOutputInt32 (called 4 times)
- Called from (representative examples):
  - pqTraceOutputMessage

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Part of the PostgreSQL frontend tracing system for debugging client-server protocol communication
- First reads a 16-bit integer indicating the number of fields/columns
- For each field, reads the following metadata in order:
  1. Field name (string)
  2. Table OID (32-bit integer, affected by regress flag)
  3. Column attribute number (16-bit integer)
  4. Type OID (32-bit integer, affected by regress flag)
  5. Type size (16-bit integer)
  6. Type modifier (32-bit integer, not affected by regress flag)
  7. Format code (16-bit integer)
- The regress parameter controls whether actual OIDs or placeholder values are shown for reproducible test output
- Used to describe the structure of query results before actual data transmission
- Output format follows tab-delimited structure for easy parsing by analysis tools
- Essential for understanding the schema and data types of query results in protocol debugging