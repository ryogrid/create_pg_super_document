# bbstreamer_zstd_compressor_free

## Location
[src/bin/pg_basebackup/bbstreamer_zstd.c:242-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_zstd.c#L242-L257)

## Overview
Releases all memory and resources allocated for the zstd compressor streamer, including the zstd compression context and buffers.

## Definition

```c
static void
bbstreamer_zstd_compressor_free(bbstreamer *streamer)
```
## Detailed Description
This function performs cleanup and memory deallocation for a zstd compressor streamer instance. It follows the standard PostgreSQL pattern for resource cleanup by first freeing the next streamer in the chain, then releasing zstd-specific resources (the compression context), followed by freeing the buffer data and finally the streamer structure itself. This ensures proper cleanup of the entire streaming pipeline and prevents memory leaks.

The function is responsible for releasing the ZSTD compression context that was allocated during initialization, as well as any dynamically allocated buffer space used for compression operations.

## Parameters / Member Variables
- `*streamer`: The bbstreamer instance to free (cast to bbstreamer_zstd_frame internally)
## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_free](bbstreamer_free.md)
  - ZSTD_freeCCtx
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - This function is typically called through the bbstreamer operations table as part of the streaming pipeline cleanup

## Notes and Other Information
- This is a static function, only accessible within the bbstreamer_zstd.c file
- Follows PostgreSQL's memory management patterns using pfree for allocated memory
- Ensures proper cleanup order: next streamer first, then zstd-specific resources, then general resources
- Critical for preventing memory leaks in long-running backup operations
- Must be called after finalization to ensure all compression operations are complete
- The function assumes the streamer has been properly initialized and all resources were successfully allocated

## Simplified Source

```c
static void
bbstreamer_zstd_compressor_free(bbstreamer *streamer)
{
    bbstreamer_zstd_frame *mystreamer = (bbstreamer_zstd_frame *) streamer;

    // Free resources in proper order
    bbstreamer_free(streamer->bbs_next);  // Free next streamer first
    ZSTD_freeCCtx(mystreamer->cctx);      // Free compression context
    pfree(streamer->bbs_buffer.data);     // Free buffer data
    pfree(streamer);                      // Free streamer structure
}
```