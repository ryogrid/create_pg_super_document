# bbstreamer_zstd_decompressor_free

## Location
src/bin/pg_basebackup/bbstreamer_zstd.c: 359 - 368

## Overview
Frees all memory and resources associated with a ZSTD decompressor streamer, including the ZSTD decompression context and internal buffers.

## Definition
```c
static void bbstreamer_zstd_decompressor_free(bbstreamer *streamer)
```

## Detailed Description
This function performs complete cleanup and memory deallocation for a ZSTD decompressor streamer. It follows the standard bbstreamer cleanup pattern by first freeing any downstream streamers in the pipeline, then releasing ZSTD-specific resources (the decompression context), and finally freeing the internal buffer data and the streamer structure itself.

The function ensures proper resource management by calling the appropriate cleanup functions in the correct order to prevent memory leaks and properly release all allocated resources including ZSTD library contexts.

## Parameters / Member Variables
- `streamer`: Pointer to the base bbstreamer structure containing the ZSTD decompressor state to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md)
  - [bbstreamer_zstd_frame](bbstreamer_zstd_frame.md)
  - [bbstreamer_free](bbstreamer_free.md)
  - ZSTD_freeDCtx (ZSTD library function)
  - [pfree](../p/pfree.md) (PostgreSQL memory management function)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a static function used as a callback in the bbstreamer framework
- The cleanup order is important: downstream streamer first, then ZSTD context, then internal buffers, and finally the streamer itself
- Uses ZSTD_freeDCtx() to properly release the ZSTD decompression context allocated during streamer initialization
- Uses PostgreSQL pfree() function for memory deallocation consistent with PostgreSQL memory management practices
- Part of the resource management infrastructure in PostgreSQL base backup streaming system