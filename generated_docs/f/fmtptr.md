# fmtptr

## Location
[src/port/snprintf.c:993-1006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L993-L1006)

## Overview
Formats and outputs a pointer value using the %p format specifier in PostgreSQL's portable snprintf implementation.

## Definition

```c
static void
fmtptr(const void *value, PrintfTarget *target)
```
## Detailed Description
This function handles the formatting and output of pointer values (%p format specifier) in PostgreSQL's portable snprintf implementation. Rather than implementing pointer formatting from scratch, it delegates to the standard C library's snprintf function to perform the actual pointer-to-string conversion, then outputs the result through PostgreSQL's formatting framework.

The function uses a temporary buffer to hold the formatted pointer string, which is then passed to the output system. This approach ensures compatibility with platform-specific pointer formatting while integrating with PostgreSQL's custom printf infrastructure for error handling and output management.

## Parameters / Member Variables
- `*value`: The pointer value to be formatted and output (cast as const void*)
- `*target`: Output destination structure containing formatting state and output buffer
## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (struct type for output destination)
  - snprintf (standard C library function for formatted output)
  - [dostr](../d/dostr.md) (function to output string content to target)

- Called from (representative examples):
  - [dopr](../d/dopr.md) (main printf formatting function)
  - [flushbuffer](flushbuffer.md) (output buffer management function)

## Notes and Other Information
- Part of PostgreSQL's platform-independent printf implementation
- Uses standard C library snprintf for actual pointer formatting to ensure platform consistency
- Handles error conditions by setting target->failed flag if snprintf returns negative value
- Uses a fixed 64-byte buffer which should be sufficient for any reasonable pointer representation
- The %p format specifier typically outputs pointers in hexadecimal format with a '0x' prefix, but exact format is implementation-defined
- Function is simpler than other formatting functions because it doesn't handle width, precision, or alignment - %p format doesn't support these modifiers