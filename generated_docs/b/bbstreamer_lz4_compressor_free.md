# bbstreamer_lz4_compressor_free

## Location
src/bin/pg_basebackup/bbstreamer_lz4.c: 258 - 274

## Overview
Frees memory and resources associated with the LZ4 compressor streamer.

## Definition


## Detailed Description
This function performs cleanup and resource deallocation for the LZ4 compressor streamer. It recursively frees the next streamer in the chain, releases the LZ4 compression context, deallocates the internal buffer, and frees the streamer structure itself.

The cleanup follows a specific order: downstream streamers are freed first, then LZ4-specific resources (compression context), followed by internal buffers, and finally the streamer structure. This ensures proper cleanup of the entire streaming chain.

## Parameters / Member Variables
- : The LZ4 compressor streamer instance to free

## Dependencies
- Functions called/Symbols referenced:
  - bbstreamer_free
  - LZ4F_freeCompressionContext
  - pfree
- Called from (representative examples):
  - bbstreamer operation table (via function pointer)

## Notes and Other Information
- Must be called to prevent memory leaks
- Recursively frees the entire downstream streamer chain
- Releases LZ4 compression context to avoid LZ4 library memory leaks  
- Follows proper cleanup order: downstream first, then local resources
- Part of the resource management system for backup streaming operations