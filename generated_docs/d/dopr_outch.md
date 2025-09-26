# dopr_outch

## Location
[src/port/snprintf.c:1411-1426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1411-L1426)

## Overview
A fundamental output function that writes a single character to a PrintfTarget, handling buffer overflow by either flushing to a stream or counting lost characters.

## Definition

```c
static void
dopr_outch(int c, PrintfTarget *target)
```
## Detailed Description
The  function is the most basic output primitive in PostgreSQL's portable snprintf implementation. It handles the output of a single character while managing buffer boundaries and providing appropriate behavior when the buffer becomes full:

- Checks if the buffer is full before writing
- If buffer is full and a stream is available, flushes the buffer automatically
- If buffer is full and no stream is available, increments the lost character count
- Writes the character to the buffer and advances the buffer pointer

This function serves as the foundation for all character output in the printf system, ensuring consistent buffer management behavior across all formatting operations.

## Parameters / Member Variables
- : The character to output (passed as int following standard C library conventions)
- : PrintfTarget structure containing the output buffer and state information

## Dependencies
- Functions called/Symbols referenced:
  - [flushbuffer](../f/flushbuffer.md)
  - PrintfTarget (structure)
- Called from:
  - [dopr](dopr.md)
  - [fmtchar](../f/fmtchar.md)
  - [pg_strfromd](../p/pg_strfromd.md)
  - [dostr](dostr.md)
  - [dopr_outchmulti](dopr_outchmulti.md)
  - [leading_pad](../l/leading_pad.md)
  - [flushbuffer](../f/flushbuffer.md)

## Notes and Other Information
- Core primitive function used throughout the printf implementation for single character output
- Takes character as int parameter following standard C library convention (allows for EOF handling in other contexts)
- Automatically manages buffer flushing when stream output is available
- When no stream is available and buffer is full, gracefully handles overflow by counting lost characters
- Buffer pointer is post-incremented after writing, maintaining correct position for subsequent writes
- Essential building block used by higher-level formatting functions and multi-character output routines