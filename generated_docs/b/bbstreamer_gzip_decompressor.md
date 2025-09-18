# bbstreamer_gzip_decompressor

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:33-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L33-L38)

## Overview
A structure representing a gzip decompression stream processor that decompresses gzip-compressed data and forwards it to the next streamer in the pipeline.

## Definition


## Detailed Description
The `bbstreamer_gzip_decompressor` is a specialized bbstreamer implementation that provides gzip decompression functionality for PostgreSQL's base backup system. It inherits from the base `bbstreamer` structure and uses zlib's z_stream for handling compressed data streams. This structure operates as part of a streaming pipeline, where it receives compressed data, decompresses it using inflate operations, and forwards the decompressed data to the next streamer in the chain.

The decompressor maintains an internal buffer and tracks the number of bytes written to manage the decompression process efficiently. It uses zlib's inflateInit2 with specific window bits (15 + 16) to handle gzip headers properly. The structure processes data in chunks, decompressing input data until the output buffer is full, then forwarding the decompressed data to the next streamer.

## Parameters / Member Variables
- `base`: The base bbstreamer structure containing common streamer functionality, operation function pointers, next streamer reference, and internal buffer
- `zstream`: The zlib z_stream structure that maintains the state of the decompression operation, including input/output pointers and buffer information
- `bytes_written`: A counter tracking the number of bytes written to the output buffer, used to determine when the buffer is full and needs to be flushed

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - z_stream (zlib type)
  - inflate (zlib function)
  - inflateInit2 (zlib function)
- Called from (representative examples):
  - [bbstreamer_gzip_decompressor_new](bbstreamer_gzip_decompressor_new.md)
  - [bbstreamer_gzip_decompressor_content](bbstreamer_gzip_decompressor_content.md)
  - [bbstreamer_gzip_decompressor_finalize](bbstreamer_gzip_decompressor_finalize.md)

## Notes and Other Information
- This structure is only available when PostgreSQL is compiled with zlib support (HAVE_LIBZ)
- The structure is typically instantiated through `bbstreamer_gzip_decompressor_new()` which initializes the zlib stream with custom memory allocation functions
- Uses `inflateInit2` with windowBits value of 15 + 16 to properly handle gzip headers
- Implements streaming decompression, processing data in chunks rather than requiring the entire compressed stream in memory
- Automatically forwards decompressed data to the next streamer when the output buffer becomes full
- Includes error handling for decompression failures with appropriate logging
- Memory management uses PostgreSQL's custom allocation functions (`gzip_palloc` and `gzip_pfree`) for zlib operations