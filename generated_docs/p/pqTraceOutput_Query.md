# pqTraceOutput_Query

## Location
[src/interfaces/libpq/fe-trace.c:421-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L421-L427)

## Overview
Outputs a formatted trace message for Query protocol messages in PostgreSQL client tracing.

## Definition
```c
static void pqTraceOutput_Query(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure, specifically designed to format and output Query protocol messages. It writes a "Query" tab-delimited header followed by the SQL query string to the trace output file. The function is used internally by the tracing system to provide human-readable logging of client-server communication.

## Parameters / Member Variables
- `f`: File pointer to the trace output destination
- `message`: Raw protocol message buffer containing the query data
- `cursor`: Pointer to current position in the message buffer (updated as data is read)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputString](pqTraceOutputString.md)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Part of the PostgreSQL frontend tracing system for debugging client-server protocol communication
- The function advances the cursor position through the message buffer as it reads the query string
- Output format follows tab-delimited structure for easy parsing by analysis tools

## Simplified Source

```c
static void
pqTraceOutput_Query(FILE *f, const char *message, int *cursor)
{
    // Output message type and SQL query
    fprintf(f, "Query\t");
    pqTraceOutputString(f, message, cursor, false);
}
```