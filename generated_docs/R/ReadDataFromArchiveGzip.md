# ReadDataFromArchiveGzip

## Location
[src/bin/pg_dump/compress_gzip.c:163-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L163-L229)

## Overview
Reads and decompresses gzip-compressed data from a PostgreSQL archive using zlib's inflate functionality.

## Definition

```c
static void
ReadDataFromArchiveGzip(ArchiveHandle *AH, CompressorState *cs)
```
## Detailed Description
This function handles the decompression of gzip-compressed data during the reading phase of PostgreSQL's pg_dump/pg_restore operations. It initializes a zlib decompression stream, reads compressed data in chunks through the CompressorState's readF function, and decompresses the data using zlib's inflate() function. The decompressed data is then written to the archive using ahwrite(). The function includes proper error handling for decompression failures and ensures complete decompression by continuing to call inflate() until Z_STREAM_END is reached.

## Parameters / Member Variables
- : Archive handle containing the archive state and operations
- : Compressor state containing the read function and buffer management

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - inflateInit
  - inflate
  - inflateEnd
  - [ahwrite](../a/ahwrite.md)
  - [pg_fatal](../p/pg_fatal.md)
  - DEFAULT_IO_BUFFER_SIZE
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses DEFAULT_IO_BUFFER_SIZE for both input and output buffers
- Implements a two-phase decompression: first reading available input data, then flushing any remaining compressed data
- Properly handles zlib error codes and provides detailed error messages
- Memory management includes allocation and deallocation of zlib stream structure and buffers
- Null-terminates output buffer for safety before writing to archive