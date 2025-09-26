# shared_buffer_write_error_callback

## Location
src/backend/storage/buffer/bufmgr.c: 5668 - 5687

## Overview
Provides error context information for errors occurring during shared buffer write operations in PostgreSQL buffer management.

## Definition
```c
static void shared_buffer_write_error_callback(void *arg)
```

## Detailed Description
This function serves as an error context callback specifically designed to provide meaningful error information when shared buffer write operations fail. When registered as an error callback, it extracts buffer metadata from the provided BufferDesc argument and formats contextual information about which block and relation were being written when the error occurred. The function safely reads the buffer tag information (since the buffer is pinned during write operations) and constructs a human-readable error message that includes the block number and relation file path.

## Parameters / Member Variables
- `arg`: A void pointer that should point to a BufferDesc structure representing the buffer being written when the error occurred

## Dependencies
- Functions called/Symbols referenced:
  - BufferDesc (structure type)
  - relpathperm (converts relation file locator to permanent path)
  - BufTagGetRelFileLocator (extracts relation file locator from buffer tag)
  - BufTagGetForkNum (extracts fork number from buffer tag)
  - errcontext (adds context information to error reports)
- Called from (representative examples):
  - BufferIsPinned
  - FlushBuffer

## Notes and Other Information
- This is a static function internal to bufmgr.c
- The function assumes the buffer is pinned, allowing safe access to the tag without spinlock protection
- Memory allocated by relpathperm() is properly freed using pfree()
- Provides critical debugging information for buffer I/O failures
- Part of PostgreSQL error reporting infrastructure for buffer management operations