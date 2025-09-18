# bbstreamer_zstd_decompressor_content

## Location
src/bin/pg_basebackup/bbstreamer_zstd.c: 296 - 337

## Overview
Decompresses input data using ZSTD compression algorithm and forwards the decompressed data to the next streamer in the pipeline when the output buffer becomes full.

## Definition
```c
static void bbstreamer_zstd_decompressor_content(bbstreamer *streamer,
                                                bbstreamer_member *member,
                                                const char *data, int len,
                                                bbstreamer_archive_context context)
```

## Detailed Description
This function is the main content processing routine for ZSTD decompression in PostgreSQL base backup streaming. It continuously decompresses input data from the `data` buffer using the ZSTD decompression context stored in the streamer. When the internal output buffer reaches capacity, it forwards the decompressed data to the next streamer in the pipeline and resets the buffer for continued processing.

The function uses ZSTD_decompressStream() to perform the actual decompression work and includes error handling for decompression failures. The decompression process continues until all input data has been consumed, making multiple passes through the output buffer as needed.

## Parameters / Member Variables
- `streamer`: Pointer to the base bbstreamer structure containing the ZSTD decompressor state
- `member`: Pointer to the current archive member being processed (used when forwarding to next streamer)
- `data`: Input buffer containing compressed data to be decompressed
- `len`: Length of the input data buffer in bytes
- `context`: Archive context information passed through to the next streamer

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md)
  - bbstreamer_member
  - [bbstreamer_archive_context](bbstreamer_archive_context.md)
  - [bbstreamer_zstd_frame](bbstreamer_zstd_frame.md)
  - [bbstreamer_content](bbstreamer_content.md)
  - ZSTD_decompressStream (ZSTD library function)
  - ZSTD_isError (ZSTD library function)
  - ZSTD_getErrorName (ZSTD library function)
  - pg_log_error
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a static function used as a callback in the bbstreamer framework
- Error handling logs decompression failures but does not abort the process
- The function manages buffer overflow by forwarding data to the next streamer when the output buffer fills up
- Uses ZSTD_inBuffer and ZSTD_outBuffer structures to manage input/output buffers efficiently
- Part of the PostgreSQL base backup streaming infrastructure for handling compressed backup data