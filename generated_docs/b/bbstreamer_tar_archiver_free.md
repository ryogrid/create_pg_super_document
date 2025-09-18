# bbstreamer_tar_archiver_free

## Location
src/bin/pg_basebackup/bbstreamer_tar.c: 451 - 461

## Overview
Frees memory associated with a tar archiver bbstreamer, including cleanup of the next streamer in the chain.

## Definition
```c
static void bbstreamer_tar_archiver_free(bbstreamer *streamer)
```

## Detailed Description
This function implements the memory cleanup logic for the tar archiver bbstreamer. It follows the standard bbstreamer cleanup pattern by first freeing any chained streamers (via bbstreamer_free on the next component) and then freeing the memory allocated for the current streamer instance itself using pfree.

This ensures proper cleanup ordering where dependent components are freed before their dependencies, preventing memory leaks and use-after-free issues in the bbstreamer processing pipeline.

## Parameters / Member Variables
- `streamer`: The tar archiver bbstreamer instance to free

## Dependencies
- Functions called/Symbols referenced:
  - bbstreamer_free (frees the next streamer in chain)
  - pfree (PostgreSQL memory deallocation)
- Called from (representative examples):
  - Via bbstreamer_tar_archiver_ops.free function pointer  
  - Through general bbstreamer cleanup mechanisms

## Notes and Other Information
- Follows the standard bbstreamer cleanup pattern: free dependencies first, then self
- Uses PostgreSQL's pfree() for memory management consistency
- Part of the bbstreamer operation contract requiring each component to implement proper cleanup
- Critical for preventing memory leaks in long-running backup operations