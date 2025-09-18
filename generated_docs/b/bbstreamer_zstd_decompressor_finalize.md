# bbstreamer_zstd_decompressor_finalize

## Location
src/bin/pg_basebackup/bbstreamer_zstd.c: 338 - 358

## Overview
Performs end-of-stream processing for ZSTD decompression by forwarding any remaining buffered data to the next streamer and finalizing the downstream pipeline.

## Definition
```c
static void bbstreamer_zstd_decompressor_finalize(bbstreamer *streamer)
```

## Detailed Description
This function handles the finalization phase of ZSTD decompression in the PostgreSQL base backup streaming pipeline. When the decompression stream reaches its end, there may still be decompressed data remaining in the output buffer that needs to be processed. The function checks if any data is pending in the output buffer and forwards it to the next streamer in the pipeline before calling the finalization routine on the downstream streamer.

This ensures that no decompressed data is lost during the finalization process and that all streamers in the pipeline are properly notified of the end-of-stream condition.

## Parameters / Member Variables
- `streamer`: Pointer to the base bbstreamer structure containing the ZSTD decompressor state

## Dependencies
- Functions called/Symbols referenced:
  - bbstreamer
  - bbstreamer_zstd_frame
  - bbstreamer_content
  - BBSTREAMER_UNKNOWN
  - bbstreamer_finalize
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a static function used as a callback in the bbstreamer framework
- The function passes NULL as the member parameter when forwarding remaining data since no specific archive member is being processed during finalization
- Uses BBSTREAMER_UNKNOWN context when forwarding data, indicating the archive context is not applicable at finalization
- Part of the cleanup phase in the PostgreSQL base backup streaming infrastructure
- Ensures proper resource cleanup by calling bbstreamer_finalize on the next streamer in the pipeline