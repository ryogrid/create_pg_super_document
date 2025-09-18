# LZ4Stream_read_internal

## Location
src/bin/pg_dump/compress_lz4.c: 451 - 572

## Overview
The core function for reading decompressed content from an LZ4 compressed stream, handling buffering, overflow management, and both binary and line-oriented read operations.

## Definition
```c
static int
LZ4Stream_read_internal(LZ4State *state, void *ptr, int ptrsize, bool eol_flag)
```

## Detailed Description
This is the primary workhorse function for LZ4 decompression in PostgreSQL's pg_dump utility. It orchestrates the complete decompression process, from lazy initialization to final data delivery. The function first attempts to satisfy read requests from the overflow buffer using LZ4Stream_read_overflow(). If more data is needed, it reads compressed data from the input stream in chunks, decompresses it using the LZ4F_decompress() library function, and manages the complex buffering logic. When decompression produces more data than requested, excess data is stored in the overflow buffer for future reads. The function supports both binary reads and line-oriented reads (when eol_flag is true), stopping at newline characters as needed. It includes comprehensive error handling for both I/O operations and LZ4 decompression errors, with proper buffer management including dynamic reallocation as needed.

## Parameters / Member Variables
- `state`: Pointer to the LZ4State structure containing decompression context and buffers
- `ptr`: Destination buffer where decompressed data will be written
- `ptrsize`: Maximum number of bytes to read into the destination buffer
- `eol_flag`: Boolean flag indicating whether to stop at the first newline character encountered

## Dependencies
- Functions called/Symbols referenced:
  - LZ4Stream_init (internal initialization function)
  - LZ4Stream_read_overflow (overflow buffer management function)
  - LZ4F_decompress (LZ4 library decompression function)
  - LZ4F_isError (LZ4 library error checking function)
  - LZ4F_getErrorName (LZ4 library error message function)
  - fread (standard C library I/O function)
  - feof (standard C library EOF checking function)
  - memchr (standard C library character searching function)
  - memcpy (standard C library memory copying function)
  - memmove (standard C library overlapping memory move function)
  - memset (standard C library memory initialization function)
  - pg_malloc (PostgreSQL memory allocation function)
  - pg_realloc (PostgreSQL memory reallocation function)
  - pg_free (PostgreSQL memory deallocation function)
  - pg_log_error (PostgreSQL logging function)
- Types referenced:
  - LZ4State
- Called from:
  - LZ4Stream_read (at compress_lz4.c:615)
  - LZ4Stream_getc (at compress_lz4.c:630) 
  - LZ4Stream_gets (at compress_lz4.c:650)

## Notes and Other Information
- This function implements sophisticated buffer management, including dynamic buffer resizing and overflow handling to accommodate varying decompression ratios
- The overflow buffer automatically grows (doubles in size) when needed to handle large decompressed blocks
- When eol_flag is set, the function carefully tracks whether a newline has been found to avoid reading beyond line boundaries
- The function performs lazy initialization by calling LZ4Stream_init() if the state hasn't been initialized yet
- Error handling includes both LZ4-specific errors (stored in state->errcode) and I/O errors (using standard errno)
- The decompression loop continues until either the requested amount of data is read, EOF is reached, or a newline is found (when eol_flag is true)
- Memory management uses PostgreSQL's allocation functions (pg_malloc, pg_realloc, pg_free) for consistent error handling
- The function handles partial reads from fread() and distinguishes between EOF and actual read errors