# bbstreamer_zstd_compressor_content

## Location
[src/bin/pg_basebackup/bbstreamer_zstd.c:145-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_zstd.c#L145-L189)

## Overview
Compresses input data using Zstandard compression and manages the output buffer, forwarding compressed data to the next streamer when the buffer becomes full.

## Definition

```c
static void
bbstreamer_zstd_compressor_content(bbstreamer *streamer,
								   bbstreamer_member *member,
								   const char *data, int len,
								   bbstreamer_archive_context context)
```
## Detailed Description
This function is the core compression routine for the zstd compressor streamer. It takes input data and compresses it using the ZSTD_compressStream2 function in streaming mode. The function implements a buffer management strategy where it calculates the maximum space needed for compression using ZSTD_compressBound, and if the current output buffer doesn't have sufficient space, it forwards the existing compressed data to the next streamer and resets the buffer.

The compression is performed iteratively until all input data is processed. The function uses ZSTD_e_continue mode for streaming compression, which allows for efficient processing of data chunks without finalizing the compression stream.

## Parameters / Member Variables
- `*streamer`: The bbstreamer instance (cast to bbstreamer_zstd_frame internally)
- `*member`: Information about the current archive member being processed
- `*data`: Pointer to the input data to be compressed
- `len`: Length of the input data in bytes
- `context`: Archive context information for the current operation
## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_compressBound
  - ZSTD_compressStream2
  - ZSTD_isError
  - ZSTD_getErrorName
  - [bbstreamer_content](bbstreamer_content.md)
  - pg_log_error
- Called from (representative examples):
  - This function is typically called through the bbstreamer operations table as part of the streaming pipeline

## Notes and Other Information
- This is a static function, only accessible within the bbstreamer_zstd.c file
- Uses streaming compression mode (ZSTD_e_continue) rather than single-shot compression
- Implements intelligent buffer management by calculating compression bounds before attempting compression
- Handles buffer overflow by forwarding data to the next streamer and resetting the output buffer
- Error handling includes checking for zstd compression errors and logging them appropriately
- The function processes all input data in a loop, ensuring complete compression of the provided data chunk