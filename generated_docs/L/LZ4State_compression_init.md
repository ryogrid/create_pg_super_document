# LZ4State_compression_init

## Location
[src/bin/pg_dump/compress_lz4.c:102-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L102-L144)

## Overview
Initializes the required LZ4State members for compression operations and writes the LZ4 frame header to a buffer for later use by pg_dump's LZ4 compression functionality.

## Definition
```c
static bool LZ4State_compression_init(LZ4State *state)
```

## Detailed Description
This function prepares an LZ4State structure for compression by setting up the compression context, allocating necessary buffers, and generating the LZ4 frame header. It calculates the optimal buffer size using LZ4F_compressBound() with the default I/O buffer size, ensuring the buffer meets the minimum requirement of LZ4F_HEADER_SIZE_MAX. The function creates an LZ4 compression context and writes the frame header to the allocated buffer, storing its length for later use. The header can be written to the output stream at the caller's discretion.

## Parameters / Member Variables
- `state`: Pointer to the LZ4State structure to initialize for compression operations

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressBound
  - LZ4F_createCompressionContext
  - LZ4F_compressBegin
  - LZ4F_isError
  - [pg_malloc](../p/pg_malloc.md)
- Constants used:
  - DEFAULT_IO_BUFFER_SIZE
  - LZ4F_HEADER_SIZE_MAX
  - LZ4F_VERSION
- Called from (representative examples):
  - [LZ4Stream_init](LZ4Stream_init.md)

## Notes and Other Information
- Returns true on success, false on failure
- On failure, stores the LZ4 error code in state->errcode for diagnostic purposes
- The function ensures the buffer size meets LZ4's minimum header size requirements
- The generated header is stored in state->buffer with length in state->compressedlen
- This is a static function internal to the compress_lz4.c module
- Part of PostgreSQL's pg_dump LZ4 compression implementation