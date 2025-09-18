# bbstreamer_tar_terminator_finalize

## Location
src/bin/pg_basebackup/bbstreamer_tar.c: 496 - 509

## Overview
A static function that finalizes a TAR archive by adding two required NUL byte blocks that the server fails to supply, ensuring proper TAR format compliance.

## Definition
```c
static void bbstreamer_tar_terminator_finalize(bbstreamer *streamer)
```

## Detailed Description
This function implements the finalization callback for the TAR terminator bbstreamer. Its primary purpose is to address a specific issue where the PostgreSQL server does not provide the required two blocks of NUL bytes that should terminate a valid TAR archive according to the TAR format specification. The function creates a buffer of exactly 2 * TAR_BLOCK_SIZE bytes, fills it with zeros using memset(), and forwards this terminator data to the next bbstreamer in the chain. After sending the termination blocks, it calls bbstreamer_finalize() on the next bbstreamer to complete the finalization process.

This ensures that the resulting TAR archive is properly formatted and can be processed by standard TAR utilities that expect the correct termination sequence.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance representing this TAR terminator

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_finalize](bbstreamer_finalize.md)
  - TAR_BLOCK_SIZE
  - BBSTREAMER_UNKNOWN
- Called from (representative examples):
  - Set as callback in bbstreamer_tar_terminator_ops.finalize

## Notes and Other Information
- This is a static function used as a callback in the bbstreamer_tar_terminator_ops structure
- Creates exactly 2 * TAR_BLOCK_SIZE (1024) bytes of NUL data as required by TAR format
- Located in src/bin/pg_basebackup/bbstreamer_tar.c:496-509
- Essential for creating valid TAR archives when using pg_basebackup
- Addresses a server-side limitation in TAR termination handling