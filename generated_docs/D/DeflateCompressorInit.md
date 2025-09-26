# DeflateCompressorInit

## Location
[src/bin/pg_dump/compress_gzip.c:46-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L46-L79)

## Overview
Initializes the deflate compression state for gzip compression in PostgreSQL's pg_dump utility.

## Definition

```c
static void
DeflateCompressorInit(CompressorState *cs)
```
## Detailed Description
This function sets up the zlib deflate compression infrastructure for pg_dump's gzip compression functionality. It allocates and initializes the necessary data structures including the z_stream for zlib operations and output buffers. The function configures the compression level as specified in the compression specification and prepares the compressor for subsequent write operations.

The function allocates a GzipCompressorState structure to maintain gzip-specific state and initializes the z_stream with default memory allocation functions. It also sets up an output buffer with a configurable size plus one extra byte for potential trailing zero bytes.

## Parameters / Member Variables
- : CompressorState pointer containing compression specifications including the desired compression level

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (for allocating zeroed memory)
  - [pg_malloc](../p/pg_malloc.md) (for memory allocation)
  - deflateInit (zlib function to initialize deflate stream)
  - [pg_fatal](../p/pg_fatal.md) (for fatal error reporting)
- Types referenced:
  - [CompressorState](../C/CompressorState.md)
  - [GzipCompressorState](../G/GzipCompressorState.md)
  - z_streamp
  - z_stream
  - DEFAULT_IO_BUFFER_SIZE
- Called from (representative examples):
  - No direct references found (likely used via function pointer)

## Notes and Other Information
- The function asserts that compression level is not 0, as level 0 would use the "None" compressor rather than zlib
- Allocates one extra byte in the output buffer for routines that may append trailing zero bytes
- Sets up paranoid initialization of zlib stream pointers to handle cases where End might be called after Start without any Write operations
- Uses pg_fatal for error handling if zlib initialization fails
- The function is static and located in src/bin/pg_dump/compress_gzip.c:46-79