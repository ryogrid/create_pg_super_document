# LZ4Stream_read_overflow

## Location
src/bin/pg_dump/compress_lz4.c: 409 - 450

## Overview
Reads already decompressed content from the internal overflow buffer, optionally stopping at newline characters for line-oriented operations.

## Definition
```c
static int
LZ4Stream_read_overflow(LZ4State *state, void *ptr, int size, bool eol_flag)
```

## Detailed Description
This static function manages the overflow buffer mechanism used in LZ4 decompression streams. When decompression operations produce more data than the caller requested, the excess data is stored in an overflow buffer for subsequent read operations. This function retrieves data from that overflow buffer, copying it to the caller's buffer. It supports both binary and line-oriented reading modes. When eol_flag is true, it stops at the first newline character within the requested size, making it suitable for line-by-line reading operations. After reading data, it compacts the overflow buffer by moving any remaining unread content to the beginning, maintaining efficient buffer usage.

## Parameters / Member Variables
- `state`: Pointer to the LZ4State structure containing the overflow buffer and related metadata
- `ptr`: Destination buffer where the overflow data will be copied
- `size`: Maximum number of bytes to read from the overflow buffer
- `eol_flag`: Boolean flag indicating whether to stop at the first newline character encountered

## Dependencies
- Functions called/Symbols referenced:
  - memchr (standard C library function for character searching)
  - memcpy (standard C library function for memory copying)
  - memmove (standard C library function for overlapping memory moves)
- Types referenced:
  - LZ4State
- Called from:
  - LZ4Stream_read_internal (at compress_lz4.c:480)

## Notes and Other Information
- This function implements part of the buffering strategy for LZ4 decompression, allowing the system to handle cases where decompression produces more data than immediately needed
- The overflow buffer management is crucial for maintaining data integrity across multiple read operations
- When eol_flag is set and a newline is found, the function includes the newline character in the returned data (readlen = p - state->overflowbuf + 1)
- The function uses memmove() instead of memcpy() for buffer compaction because the source and destination memory regions may overlap
- Returns 0 when the overflow buffer is empty, allowing callers to determine when fresh decompression is needed
- The buffer compaction ensures efficient memory usage by keeping unread data at the beginning of the buffer