# bbstreamer_zstd_compressor_finalize

## Location
src/bin/pg_basebackup/bbstreamer_zstd.c: 190 - 241

## Overview
Performs end-of-stream processing for the zstd compressor by flushing any remaining compressed data and finalizing the compression stream.

## Definition


## Detailed Description
This function handles the finalization phase of zstd compression when no more input data will be provided. It uses ZSTD_compressStream2 with the ZSTD_e_end flag to signal the end of the compression stream and flush any remaining data from the compressor's internal buffers. The function operates in a loop, continuing to call the compression function until all data has been flushed (indicated by yet_to_flush returning 0).

The function manages the output buffer similarly to the content function, forwarding data to the next streamer when the buffer needs space and ensuring that all remaining compressed data is sent to the next stage of the pipeline. Finally, it calls bbstreamer_finalize on the next streamer to propagate the finalization through the entire streaming chain.

## Parameters / Member Variables
- : The bbstreamer instance to finalize (cast to bbstreamer_zstd_frame internally)

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_compressBound
  - ZSTD_compressStream2
  - ZSTD_isError
  - ZSTD_getErrorName
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_finalize](bbstreamer_finalize.md)
  - pg_log_error
- Called from (representative examples):
  - This function is typically called through the bbstreamer operations table as part of the streaming pipeline finalization

## Notes and Other Information
- This is a static function, only accessible within the bbstreamer_zstd.c file
- Uses ZSTD_e_end mode to signal end-of-stream to the zstd library
- Implements a loop to ensure all internal compressor data is flushed completely
- Handles buffer management by forwarding data when space is needed for the final flush
- Ensures any remaining data in the output buffer is forwarded to the next streamer
- Properly propagates finalization to the next streamer in the pipeline
- Uses BBSTREAMER_UNKNOWN context since this is end-of-stream processing without specific archive member context
- Critical for ensuring compression integrity and completing the zstd frame properly