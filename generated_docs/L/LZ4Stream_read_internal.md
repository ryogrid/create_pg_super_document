# LZ4Stream_read_internal

## Location
[src/bin/pg_dump/compress_lz4.c:451-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L451-L572)

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
  - [LZ4Stream_init](LZ4Stream_init.md) (internal initialization function)
  - [LZ4Stream_read_overflow](LZ4Stream_read_overflow.md) (overflow buffer management function)
  - LZ4F_decompress (LZ4 library decompression function)
  - LZ4F_isError (LZ4 library error checking function)
  - LZ4F_getErrorName (LZ4 library error message function)
  - fread (standard C library I/O function)
  - feof (standard C library EOF checking function)
  - memchr (standard C library character searching function)
  - memcpy (standard C library memory copying function)
  - memmove (standard C library overlapping memory move function)
  - memset (standard C library memory initialization function)
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation function)
  - [pg_realloc](../p/pg_realloc.md) (PostgreSQL memory reallocation function)
  - [pg_free](../p/pg_free.md) (PostgreSQL memory deallocation function)
  - pg_log_error (PostgreSQL logging function)
- Types referenced:
  - [LZ4State](LZ4State.md)
- Called from:
  - [LZ4Stream_read](LZ4Stream_read.md) (at compress_lz4.c:615)
  - [LZ4Stream_getc](LZ4Stream_getc.md) (at compress_lz4.c:630) 
  - [LZ4Stream_gets](LZ4Stream_gets.md) (at compress_lz4.c:650)

## Notes and Other Information
- This function implements sophisticated buffer management, including dynamic buffer resizing and overflow handling to accommodate varying decompression ratios
- The overflow buffer automatically grows (doubles in size) when needed to handle large decompressed blocks
- When eol_flag is set, the function carefully tracks whether a newline has been found to avoid reading beyond line boundaries
- The function performs lazy initialization by calling LZ4Stream_init() if the state hasn't been initialized yet
- Error handling includes both LZ4-specific errors (stored in state->errcode) and I/O errors (using standard errno)
- The decompression loop continues until either the requested amount of data is read, EOF is reached, or a newline is found (when eol_flag is true)
- Memory management uses PostgreSQL's allocation functions (pg_malloc, pg_realloc, pg_free) for consistent error handling
- The function handles partial reads from fread() and distinguishes between EOF and actual read errors

## Simplified Source

```c
static int
LZ4Stream_read_internal(LZ4State *state, void *ptr, int ptrsize, bool eol_flag)
{
    int dsize = 0;      // bytes decompressed so far
    int size = ptrsize;
    bool eol_found = false;

    // Lazy initialization for decompression
    if (!LZ4Stream_init(state, size, false)) {
        pg_log_error("unable to initialize LZ4 library: %s",
                     LZ4F_getErrorName(state->errcode));
        return -1;
    }

    if (size <= 0) return 0;

    // Ensure buffer is large enough
    if (size > state->buflen) {
        state->buflen = size;
        state->buffer = pg_realloc(state->buffer, size);
    }

    // First, try to satisfy request from overflow buffer
    dsize = LZ4Stream_read_overflow(state, ptr, size, eol_flag);
    if (dsize == size || (eol_flag && memchr(ptr, '\n', dsize))) {
        return dsize;
    }

    // Read and decompress new data as needed
    void *readbuf = pg_malloc(size);
    int rsize;

    do {
        // Read compressed data from file
        rsize = fread(readbuf, 1, size, state->fp);
        if (rsize < size && !feof(state->fp)) {
            pg_log_error("could not read from input file: %m");
            return -1;
        }

        // Process all read data
        char *rp = (char *) readbuf;
        char *rend = (char *) readbuf + rsize;

        while (rp < rend) {
            // Decompress chunk
            size_t outlen = state->buflen;
            size_t read_remain = rend - rp;

            memset(state->buffer, 0, outlen);
            size_t status = LZ4F_decompress(state->dtx, state->buffer, &outlen,
                                           rp, &read_remain, NULL);
            if (LZ4F_isError(status)) {
                state->errcode = status;
                pg_log_error("could not read from input file: %s",
                             LZ4F_getErrorName(state->errcode));
                return -1;
            }
            rp += read_remain;

            // Copy decompressed data to output buffer
            if (outlen > 0 && dsize < size && !eol_found) {
                size_t lib = (!eol_flag) ? size - dsize : size - 1 - dsize;
                size_t len = outlen < lib ? outlen : lib;

                // Check for newline if in line mode
                if (eol_flag) {
                    char *p = memchr(state->buffer, '\n', outlen);
                    if (p && (size_t)(p - state->buffer + 1) <= len) {
                        len = p - state->buffer + 1;
                        eol_found = true;
                    }
                }

                memcpy((char *) ptr + dsize, state->buffer, len);
                dsize += len;

                // Move remaining data to front of buffer
                if (len < outlen) {
                    memmove(state->buffer, state->buffer + len, outlen - len);
                }
                outlen -= len;
            }

            // Store overflow data for future reads
            if (outlen > 0) {
                // Grow overflow buffer if needed
                while (state->overflowlen + outlen > state->overflowalloclen) {
                    state->overflowalloclen *= 2;
                    state->overflowbuf = pg_realloc(state->overflowbuf,
                                                   state->overflowalloclen);
                }
                memcpy(state->overflowbuf + state->overflowlen, state->buffer, outlen);
                state->overflowlen += outlen;
            }
        }
    } while (rsize == size && dsize < size && !eol_found);

    pg_free(readbuf);
    return dsize;
}
```