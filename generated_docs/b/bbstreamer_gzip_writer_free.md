# bbstreamer_gzip_writer_free

## Location
src/bin/pg_basebackup/bbstreamer_gzip.c: 177 - 193

## Overview
Frees all memory and resources associated with a gzip writer bbstreamer instance, ensuring proper cleanup after backup completion.

## Definition
```c
static void bbstreamer_gzip_writer_free(bbstreamer *streamer)
```

## Detailed Description
This static function handles the memory deallocation phase of the gzip writer bbstreamer lifecycle. It performs final cleanup by freeing the pathname string and the streamer structure itself. The function includes assertions to verify that the streamer is in a proper state for cleanup - specifically that it has no next streamer in the pipeline and that the gzip file has already been closed.

This function should only be called after bbstreamer_gzip_writer_finalize() has been called to properly close the gzip file. The assertions help catch programming errors where the cleanup order is incorrect or where the streamer is being freed while still in use.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance to free (cast to bbstreamer_gzip_writer internally)

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - pfree
- Called from (representative examples):
  - Used as callback through bbstreamer_gzip_writer_ops function pointer table

## Notes and Other Information
- Asserts that bbs_next is NULL, ensuring no dangling pipeline connections
- Asserts that gzfile is NULL, ensuring proper finalization occurred first
- Frees the duplicated pathname string that was allocated in the constructor
- Part of the static callback interface for complete resource cleanup
- Critical for preventing memory leaks in long-running backup operations