# pqTraceOutputNchar

## Location
[src/interfaces/libpq/fe-trace.c:188-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L188-L218)

## Overview
A static utility function that reads a fixed-length byte sequence from a PostgreSQL protocol message buffer and outputs it to the trace log with proper handling of non-printable characters.

## Definition
```c
static void pqTraceOutputNchar(FILE *pfdebug, int len, const char *data, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure. It extracts exactly 'len' bytes from the protocol message data starting at the current cursor position and writes them to the trace output file enclosed in single quotes. Unlike pqTraceOutputString, this function reads a fixed number of bytes regardless of null terminators.

The function intelligently handles non-printable characters by escaping them as hexadecimal sequences (\\xNN), while printable characters are output as-is. This approach provides readable output for binary data while preserving the exact content for debugging purposes.

## Parameters / Member Variables
- `pfdebug`: FILE pointer to the trace output file where the byte sequence will be written
- `len`: The exact number of bytes to read from the data buffer
- `data`: Pointer to the message buffer containing the binary protocol data  
- `cursor`: Pointer to the current position in the data buffer; updated by 'len' bytes after reading

## Dependencies
- Functions called/Symbols referenced:
  - (Uses standard C library functions: fprintf, fwrite, isprint)
- Called from (representative examples):
  - [pqTraceOutput_Bind](pqTraceOutput_Bind.md)
  - [pqTraceOutput_DataRow](pqTraceOutput_DataRow.md)
  - [pqTraceOutput_FunctionCall](pqTraceOutput_FunctionCall.md)
  - [pqTraceOutput_FunctionCallResponse](pqTraceOutput_FunctionCallResponse.md)

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Output is always enclosed in single quotes to distinguish from string outputs (which use double quotes)
- Non-printable characters are escaped as \\xNN hexadecimal sequences for clarity
- The function efficiently processes runs of printable characters using fwrite() for performance
- Unlike pqTraceOutputString, this function does not have a suppress parameter since it's typically used for binary data
- Used primarily for outputting parameter values and binary data where the exact byte content is important
- The function advances the cursor by exactly the specified length, enabling precise parsing of fixed-length protocol fields

## Simplified Source

```c
static void pqTraceOutputNchar(FILE *pfdebug, int len, const char *data, int *cursor)
{
    int i, next = 0;  // next = first char not yet printed
    const char *v = data + *cursor;

    fprintf(pfdebug, " '");

    // Process characters, escaping non-printable ones
    for (i = 0; i < len; ++i) {
        if (isprint((unsigned char) v[i])) {
            // Continue accumulating printable characters
            continue;
        } else {
            // Output any accumulated printable characters
            fwrite(v + next, 1, i - next, pfdebug);

            // Output non-printable character as hex escape
            fprintf(pfdebug, "\\x%02x", v[i]);
            next = i + 1;
        }
    }

    // Output any remaining printable characters
    if (next < len)
        fwrite(v + next, 1, len - next, pfdebug);

    fprintf(pfdebug, "'");
    *cursor += len;  // Advance cursor by exactly len bytes
}
```