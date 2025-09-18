# bbstreamer_tar_parser_finalize

## Location
src/bin/pg_basebackup/bbstreamer_tar.c: 319 - 340

## Overview
Performs end-of-stream processing for a tar parser, validating the final state and forwarding any remaining archive trailer data.

## Definition
```c
static void bbstreamer_tar_parser_finalize(bbstreamer *streamer)
```

## Detailed Description
This function handles the finalization phase of tar parsing when the input stream has ended. It first validates that the parser is in an appropriate final state - either expecting an archive trailer or expecting a new member header with no buffered data. If the parser is in an invalid state (such as in the middle of processing file contents), it raises a fatal error indicating the COPY stream ended prematurely. After validation, it forwards any buffered data as an archive trailer to the next bbstreamer in the chain, even if the buffer is empty. Finally, it propagates the finalization call to the next bbstreamer to ensure proper cleanup throughout the processing chain.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance being finalized, cast internally to bbstreamer_tar_parser

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_finalize](bbstreamer_finalize.md)
  - [pg_fatal](../p/pg_fatal.md)
  - BBSTREAMER_ARCHIVE_TRAILER
  - BBSTREAMER_MEMBER_HEADER
- Called from (representative examples):
  - No direct references found (likely called through function pointer in ops structure)

## Notes and Other Information
- Validates parser state to ensure the stream ended at an appropriate boundary
- Acceptable final states are BBSTREAMER_ARCHIVE_TRAILER or BBSTREAMER_MEMBER_HEADER with empty buffer
- Sends archive trailer data even if the buffer is empty to maintain protocol consistency
- Propagates finalization to the next bbstreamer in the processing chain
- Uses pg_fatal for unrecoverable error conditions when stream ends unexpectedly
- Critical for proper cleanup and error detection in the backup streaming pipeline