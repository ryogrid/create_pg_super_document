# bbstreamer_lz4_decompressor_free

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:412-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L412-L422)

## Overview
Releases all resources associated with an LZ4 decompressor streamer, including LZ4 decompression context, buffers, and the streamer chain.

## Definition
```c
static void bbstreamer_lz4_decompressor_free(bbstreamer *streamer)
```

## Detailed Description
This function performs complete cleanup and memory deallocation for an LZ4 decompression streamer. It follows a systematic cleanup order: first freeing the next streamer in the chain (ensuring proper cleanup of the entire pipeline), then releasing the LZ4-specific decompression context, followed by the internal data buffer, and finally the streamer object itself.

The cleanup sequence is important for avoiding memory leaks and ensuring that all components of the streaming decompression system are properly released. The function uses the LZ4 library's official cleanup function for the decompression context and PostgreSQL's pfree for memory allocated through PostgreSQL's memory management system.

## Parameters / Member Variables
- `streamer`: Pointer to the base bbstreamer object to be freed, internally cast to bbstreamer_lz4_frame to access LZ4-specific resources

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_free](bbstreamer_free.md) (recursively frees the next streamer in the processing chain)
  - LZ4F_freeDecompressionContext (external LZ4 library function for context cleanup)
  - [pfree](../p/pfree.md) (PostgreSQL memory management function for buffer and object deallocation)
- Called from (representative examples):
  - Referenced indirectly through bbstreamer function pointer mechanism during cleanup

## Notes and Other Information
- This is a static function used internally within the LZ4 streaming decompressor implementation
- Must be called exactly once when the decompressor is no longer needed to prevent memory leaks
- The cleanup order is critical: next streamer first, then LZ4 context, then buffers, then the object itself
- Assumes all processing has completed and no further decompression operations will be performed
- Part of the standard bbstreamer cleanup protocol where each streamer is responsible for cleaning up both its own resources and propagating cleanup down the processing chain
- Uses PostgreSQL's memory management functions (pfree) rather than standard C library free(), consistent with PostgreSQL's memory context system