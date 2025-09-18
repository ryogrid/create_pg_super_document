# bbstreamer_lz4_decompressor_new

## Location
src/bin/pg_basebackup/bbstreamer_lz4.c: 275 - 309

## Overview
Creates a new base backup streamer that performs LZ4 decompression of compressed backup blocks.

## Definition


## Detailed Description
This function initializes a new LZ4 decompression streamer for processing compressed PostgreSQL backup data. It creates and configures the LZ4 decompression context, sets up the decompressor operation table, and initializes internal buffers for handling compressed input data.

Similar to the compressor, this function only compiles when USE_LZ4 is defined and fails with a fatal error for builds without LZ4 support. It uses LZ4F_createDecompressionContext to establish the decompression state needed for processing LZ4 frame format data.

## Parameters / Member Variables
- : Pointer to the next streamer in the processing chain that will receive decompressed data

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_lz4_frame](bbstreamer_lz4_frame.md) (struct type)
  - [bbstreamer_ops](bbstreamer_ops.md) (operations table)
  - LZ4F_createDecompressionContext
  - [palloc0](../p/palloc0.md)
  - initStringInfo
- Called from (representative examples):
  - Backup restoration utilities (when decompressing LZ4 backup streams)

## Notes and Other Information
- Only available when PostgreSQL is built with LZ4 support (USE_LZ4 defined)
- Creates decompression context using LZ4F_VERSION for compatibility
- Uses pg_fatal instead of pg_log_error for context creation failures (more severe)
- Complements the compression functionality for complete backup/restore LZ4 support
- Part of the streaming decompression pipeline for restoring compressed PostgreSQL backups