# bbstreamer_tar_terminator_free

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:510-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L510-L514)

## Overview
A static function that deallocates memory associated with a TAR terminator bbstreamer, properly cleaning up the bbstreamer chain and its associated resources.

## Definition
```c
static void bbstreamer_tar_terminator_free(bbstreamer *streamer)
```

## Detailed Description
This function implements the memory cleanup callback for the TAR terminator bbstreamer. It follows the standard bbstreamer cleanup pattern by first recursively freeing the next bbstreamer in the chain using bbstreamer_free(), and then deallocating the current bbstreamer instance using pfree(). This ensures proper cleanup of the entire bbstreamer pipeline and prevents memory leaks.

The function is called when the TAR terminator bbstreamer is no longer needed, typically at the end of a backup operation or when an error occurs that requires cleanup of the streaming pipeline.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance representing this TAR terminator to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_free](bbstreamer_free.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - Set as callback in bbstreamer_tar_terminator_ops.free

## Notes and Other Information
- This is a static function used as a callback in the bbstreamer_tar_terminator_ops structure
- Follows the standard bbstreamer cleanup pattern of freeing next, then self
- Located in src/bin/pg_basebackup/bbstreamer_tar.c:510-514
- Essential for preventing memory leaks in the pg_basebackup streaming pipeline
- Part of the resource management infrastructure for TAR archive processing

## Simplified Source

```c
static void bbstreamer_tar_terminator_free(bbstreamer *streamer) {
    // Standard cleanup pattern: free chain then self
    bbstreamer_free(streamer->bbs_next);
    pfree(streamer);
}
```