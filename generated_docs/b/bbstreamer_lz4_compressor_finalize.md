# bbstreamer_lz4_compressor_finalize

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:199-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L199-L257)

## Overview
Finalizes LZ4 compression by writing the frame footer and flushing remaining data through the streaming chain.

## Definition


## Detailed Description
This function performs end-of-stream processing for LZ4 compression. It calculates the footer boundary requirements, ensures sufficient buffer space, and calls LZ4F_compressEnd to finalize the compression frame and flush any remaining data from the compression context.

The function handles buffer management by forwarding existing compressed data if there isn't enough space for the footer, dynamically resizing the buffer if needed, and ensuring all remaining compressed data is properly forwarded to the next streamer in the chain before finalizing the downstream processing.

## Parameters / Member Variables
- : The LZ4 compressor streamer instance to finalize

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressBound
  - LZ4F_compressEnd
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_finalize](bbstreamer_finalize.md)
  - enlargeStringInfo
- Called from (representative examples):
  - [bbstreamer](bbstreamer.md) operation table (via function pointer)

## Notes and Other Information
- Must be called to properly close LZ4 compression frames
- Calculates footer space requirements using LZ4F_compressBound with zero input length
- Forwards any buffered compressed data before writing footer
- Calls downstream finalization to complete the processing chain
- Essential for generating valid LZ4 compressed backup files