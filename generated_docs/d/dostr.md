# dostr

## Location
[src/port/snprintf.c:1374-1410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1374-L1410)

## Overview
A utility function that efficiently outputs string data to a PrintfTarget, handling buffer management and providing a fast path for single-character strings.

## Definition

```c
static void
dostr(const char *str, int slen, PrintfTarget *target)
```
## Detailed Description
The  function is a core output routine in PostgreSQL's portable snprintf implementation that handles the efficient transfer of string data to the target buffer or stream. It implements several optimizations:

- Fast path for single-character strings using dopr_outch() directly
- Buffer-aware copying that respects target buffer boundaries
- Automatic buffer flushing to streams when the buffer becomes full
- Graceful handling of full buffers with no stream (counts lost characters)
- Uses memmove for safe memory copying that handles overlapping regions

The function is designed to handle both buffered output (when target->bufend is set) and unbuffered stream output, making it a versatile component for various output scenarios in the printf implementation.

## Parameters / Member Variables
- : Pointer to the source string data to output
- : Length of the string data in bytes
- : PrintfTarget structure containing output buffer and state information

## Dependencies
- Functions called/Symbols referenced:
  - [dopr_outch](dopr_outch.md)
  - [flushbuffer](../f/flushbuffer.md)
  - memmove (system library)
  - PrintfTarget (structure)
- Called from:
  - [dopr](dopr.md)
  - [fmtstr](../f/fmtstr.md)
  - [fmtptr](../f/fmtptr.md)
  - [fmtint](../f/fmtint.md)
  - [fmtfloat](../f/fmtfloat.md)
  - [pg_strfromd](../p/pg_strfromd.md)
  - [flushbuffer](../f/flushbuffer.md)

## Notes and Other Information
- Provides fast path optimization for single-character strings to avoid unnecessary buffer management overhead
- Uses memmove rather than memcpy to safely handle potentially overlapping memory regions
- When buffer is full and no stream is available, it tracks the number of lost characters in target->nchars
- Buffer flushing is handled automatically when the buffer fills up and a stream is available
- Critical building block used throughout PostgreSQL's printf implementation for string output
- Maintains buffer state correctly by advancing bufptr and updating available space calculations