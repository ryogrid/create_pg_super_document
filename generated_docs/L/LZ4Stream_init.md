# LZ4Stream_init

## Location
src/bin/pg_dump/compress_lz4.c: 354 - 408

## Overview
Initializes an already allocated LZ4State structure for subsequent compression or decompression operations, setting up the necessary LZ4 contexts and buffers.

## Definition
```c
static bool
LZ4Stream_init(LZ4State *state, int size, bool compressing)
```

## Detailed Description
This static function performs the lazy initialization of an LZ4State structure, preparing it for either compression or decompression operations based on the compressing parameter. For compression operations, it calls LZ4State_compression_init() to set up the compression context and writes the LZ4 header to the output stream via fwrite(). For decompression operations, it creates a decompression context using the LZ4 library, allocates buffers for both main operations and overflow data handling, and sets appropriate buffer sizes. The function includes error handling for both LZ4-specific errors and system I/O errors, storing error codes in the state structure for later retrieval via LZ4Stream_get_error().

## Parameters / Member Variables
- `state`: Pointer to the LZ4State structure to be initialized
- `size`: Buffer size hint, used to determine decompression buffer allocation (will be at least DEFAULT_IO_BUFFER_SIZE)
- `compressing`: Boolean flag indicating whether this stream will be used for compression (true) or decompression (false)

## Dependencies
- Functions called/Symbols referenced:
  - [LZ4State_compression_init](LZ4State_compression_init.md) (internal compression setup function)
  - LZ4F_createDecompressionContext (LZ4 library function)
  - LZ4F_isError (LZ4 library error checking function)
  - fwrite (standard C library I/O function)
  - pg_malloc (PostgreSQL memory allocation function)
  - Max (PostgreSQL macro for maximum value)
- Constants referenced:
  - DEFAULT_IO_BUFFER_SIZE
  - LZ4F_VERSION (LZ4 library constant)
  - ENOSPC (system error code)
- Called from:
  - [LZ4Stream_read_internal](LZ4Stream_read_internal.md) (at compress_lz4.c:461)
  - [LZ4Stream_write](LZ4Stream_write.md) (at compress_lz4.c:580)

## Notes and Other Information
- This function implements lazy initialization - it returns immediately if the state is already initialized (state->inited == true)
- For compression, it immediately writes the LZ4 header to the output stream, which means the file pointer must be valid and writable
- For decompression, it allocates two buffers: one for main I/O operations and an overflow buffer for handling data that exceeds read requests
- The function uses PostgreSQL's pg_malloc() instead of standard malloc(), ensuring memory allocation failures are handled consistently with the rest of PostgreSQL
- Buffer size for decompression is determined by taking the maximum of the provided size parameter and DEFAULT_IO_BUFFER_SIZE
- Error handling distinguishes between LZ4 library errors (stored in errcode) and system errors (reflected in errno)