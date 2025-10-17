# LZ4Stream_init

## Location
[src/bin/pg_dump/compress_lz4.c:354-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L354-L408)

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
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation function)
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

## Simplified Source

```c
static bool
LZ4Stream_init(LZ4State *state, int size, bool compressing)
{
    // Skip if already initialized
    if (state->inited) {
        return true;
    }

    state->compressing = compressing;

    if (state->compressing) {
        // Setup compression context and write header to output stream
        if (!LZ4State_compression_init(state)) {
            return false;
        }

        // Write LZ4 header to file
        errno = 0;
        if (fwrite(state->buffer, 1, state->compressedlen, state->fp) != state->compressedlen) {
            errno = (errno) ? errno : ENOSPC;
            return false;
        }
    } else {
        // Setup decompression context
        size_t status = LZ4F_createDecompressionContext(&state->dtx, LZ4F_VERSION);
        if (LZ4F_isError(status)) {
            state->errcode = status;
            return false;
        }

        // Allocate buffers for decompression
        state->buflen = Max(size, DEFAULT_IO_BUFFER_SIZE);
        state->buffer = pg_malloc(state->buflen);

        // Allocate overflow buffer for excess data handling
        state->overflowalloclen = state->buflen;
        state->overflowbuf = pg_malloc(state->overflowalloclen);
        state->overflowlen = 0;
    }

    state->inited = true;
    return true;
}
```