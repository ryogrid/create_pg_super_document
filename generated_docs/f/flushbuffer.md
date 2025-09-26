# flushbuffer

## Location
[src/port/snprintf.c:298-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L298-L355)

## Overview
A static utility function that writes the entire contents of a formatting buffer to the target stream and resets the buffer pointer for continued use.

## Definition
```c
static void flushbuffer(PrintfTarget *target)
```

## Detailed Description
flushbuffer is an internal utility function used within PostgreSQL's portable printf implementation to manage output buffering. When the formatting buffer becomes full or when a complete formatted string needs to be output, this function attempts to write the entire buffer contents to the target stream using fwrite(). The function handles error conditions gracefully by preserving the original failure state and errno value.

The function calculates the number of characters currently in the buffer by computing the difference between the current buffer pointer (bufptr) and the buffer start (bufstart). It only attempts to write if the target hasn't previously failed and there are characters to write. After the write operation (successful or not), it always resets the buffer pointer to the beginning, effectively discarding the buffer contents to prepare for the next round of formatting.

## Parameters / Member Variables
- `target`: Pointer to a PrintfTarget structure containing the output buffer, stream information, and formatting state

## Dependencies
- Functions called/Symbols referenced:
  - fwrite (standard C library function)
  - PrintfTarget (struct type used throughout the printf implementation)
- Called from (representative examples):
  - [pg_vfprintf](../p/pg_vfprintf.md)
  - [dostr](../d/dostr.md)
  - [dopr_outch](../d/dopr_outch.md)
  - [dopr_outchmulti](../d/dopr_outchmulti.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within src/port/snprintf.c
- The function preserves error state by checking target->failed before attempting any write operations
- Updates the total character count (target->nchars) only with the number of characters actually written
- Always resets the buffer pointer regardless of write success or failure, maintaining buffer consistency
- Part of PostgreSQL's buffered output strategy that improves performance by reducing the number of system calls
- Critical for maintaining proper error handling and preserving errno values in the printf implementation