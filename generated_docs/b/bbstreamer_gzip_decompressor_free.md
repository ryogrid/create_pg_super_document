# bbstreamer_gzip_decompressor_free

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:338-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L338-L349)

## Overview
Frees all memory allocated by the gzip decompressor streamer, including the downstream streamer chain and internal buffers.

## Definition
```c
static void bbstreamer_gzip_decompressor_free(bbstreamer *streamer)
```

## Detailed Description
This function performs complete memory cleanup for a gzip decompressor streamer instance. It follows the standard cleanup protocol by first freeing the next streamer in the chain (propagating the free operation downstream), then releasing the internal buffer memory, and finally freeing the streamer structure itself. This ensures proper resource deallocation and prevents memory leaks in the streaming pipeline. The function is part of the streamer's cleanup lifecycle and is typically called when the entire streaming operation is complete or when an error occurs that requires cleanup.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance to free (should be a bbstreamer_gzip_decompressor)

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base streamer type)
  - [bbstreamer_free](bbstreamer_free.md) (function to free the next streamer in chain)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in ops structure)

## Notes and Other Information
- This is a static function, only accessible within the bbstreamer_gzip.c compilation unit
- Part of the resource management protocol for the streaming architecture
- Follows a specific cleanup order: downstream streamers first, then buffers, then self
- Uses PostgreSQL's pfree() function for memory deallocation
- Does not explicitly call inflateEnd() on the zlib stream (this may be handled elsewhere or assumed to be done during finalization)
- Critical for preventing memory leaks in long-running backup operations
- Called through the function pointer in the bbstreamer_ops structure during cleanup