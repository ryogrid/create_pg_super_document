# bbstreamer_tar_archiver_finalize

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:442-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L442-L450)

## Overview
Handles end-of-stream processing for a tar archiver by finalizing the next bbstreamer in the chain.

## Definition
```c
static void bbstreamer_tar_archiver_finalize(bbstreamer *streamer)
```

## Detailed Description
This function implements the finalization logic for the tar archiver bbstreamer. When the end of the input stream is reached, it simply delegates the finalization process to the next bbstreamer in the processing chain. This follows the typical bbstreamer pattern where each component in the pipeline handles its own cleanup and then passes control to the subsequent component.

The function is part of the bbstreamer_tar_archiver_ops operation table and is called automatically when the streaming process completes or when explicit finalization is requested.

## Parameters / Member Variables
- `streamer`: The tar archiver bbstreamer instance to finalize

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_finalize](bbstreamer_finalize.md) (finalizes the next streamer in chain)
- Called from (representative examples):
  - Via bbstreamer_tar_archiver_ops.finalize function pointer
  - Through general bbstreamer finalization mechanisms

## Notes and Other Information
- This is a simple pass-through finalization - the tar archiver doesn't require special cleanup beyond delegating to the next component
- Part of the bbstreamer operation contract where each component must implement content, finalize, and free operations
- Ensures proper cleanup ordering in the bbstreamer processing pipeline