# pqTraceOutput_FunctionCallResponse

## Location
[src/interfaces/libpq/fe-trace.c:483-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L483-L493)

## Overview
Outputs a formatted trace message for PostgreSQL's FunctionCallResponse backend message, displaying the result data length and content for function call debugging.

## Definition
```c
static void pqTraceOutput_FunctionCallResponse(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq client library tracing system that handles the parsing and output formatting of FunctionCallResponse messages received from the PostgreSQL backend. FunctionCallResponse messages are sent by the server in response to client function calls, containing the return value or result data from the executed function.

The function first reads a 32-bit integer indicating the length of the result data. If the length is not -1 (which would indicate a NULL result), it proceeds to output the actual result data using pqTraceOutputNchar(). The function handles both NULL results (len = -1) and actual data results gracefully.

## Parameters / Member Variables
- `f`: FILE pointer to the trace output destination (typically stderr or a log file)
- `message`: Pointer to the message buffer containing the raw protocol message data
- `cursor`: Pointer to an integer tracking the current read position within the message buffer; updated as data is consumed

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard C library)
  - [pqTraceOutputInt32](pqTraceOutputInt32.md) (reads and formats the result data length)
  - [pqTraceOutputNchar](pqTraceOutputNchar.md) (outputs the result data content when not NULL)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message dispatcher for trace output)

## Notes and Other Information
- This is a static function within fe-trace.c, part of the internal tracing infrastructure
- The function outputs "FunctionCallResponse" as a tab-separated label followed by the length and data
- A length value of -1 indicates a NULL function result, in which case no data content is output
- The result data can contain binary content, which pqTraceOutputNchar handles by escaping non-printable characters
- Used for debugging PostgreSQL's function call protocol between client and server
- Function calls in PostgreSQL can be invoked using the binary protocol for better performance compared to text-based SQL

## Simplified Source

```c
static void pqTraceOutput_FunctionCallResponse(FILE *f, const char *message, int *cursor)
{
    // Output message type identifier
    fprintf(f, "FunctionCallResponse\t");

    // Extract result data length (-1 means NULL)
    int len = pqTraceOutputInt32(f, message, cursor, false);

    // Output result data if not NULL
    if (len != -1) {
        pqTraceOutputNchar(f, len, message, cursor);
    }
}
```