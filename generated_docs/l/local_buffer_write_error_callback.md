# local_buffer_write_error_callback

## Location
src/backend/storage/buffer/bufmgr.c: 5688 - 5707

## Overview
Provides error context information for errors occurring during local buffer write operations in PostgreSQL buffer management.

## Definition
```c
static void local_buffer_write_error_callback(void *arg)
```

## Detailed Description
This function serves as an error context callback specifically designed to provide meaningful error information when local buffer write operations fail. Similar to its shared buffer counterpart, it extracts buffer metadata from the provided BufferDesc argument and formats contextual information about which block and relation were being written when the error occurred. The key difference is that this function uses relpathbackend() instead of relpathperm() to construct the file path, as local buffers are backend-specific temporary files that include the process number in their path structure.

## Parameters / Member Variables
- `arg`: A void pointer that should point to a BufferDesc structure representing the local buffer being written when the error occurred

## Dependencies
- Functions called/Symbols referenced:
  - BufferDesc (structure type)
  - relpathbackend (converts relation file locator to backend-specific path)
  - BufTagGetRelFileLocator (extracts relation file locator from buffer tag)
  - MyProcNumber (global variable for current process number)
  - BufTagGetForkNum (extracts fork number from buffer tag)
  - errcontext (adds context information to error reports)
- Called from (representative examples):
  - BufferIsPinned
  - FlushRelationBuffers

## Notes and Other Information
- This is a static function internal to bufmgr.c
- Handles local/temporary buffers as opposed to shared buffers
- Uses MyProcNumber to include process-specific information in the path
- Memory allocated by relpathbackend() is properly freed using pfree()
- Part of PostgreSQL error reporting infrastructure for local buffer management operations
- Provides critical debugging information for local buffer I/O failures