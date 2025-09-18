# DebugPrintBufferRefcount

## Location
src/backend/storage/buffer/bufmgr.c: 3654 - 3698

## Overview
DebugPrintBufferRefcount is a utility function that generates detailed diagnostic information about a buffer, including its identity, file path, block number, flags, and reference counts.

## Definition
char *DebugPrintBufferRefcount(Buffer buffer)

## Detailed Description
This function serves as a debugging helper that produces comprehensive diagnostic information about a specific buffer. It handles both shared and local buffers, extracting detailed metadata including the buffer's associated file path, block number, state flags, and reference counts (both shared and private). The function is primarily used by buffer leak detection routines and resource owner cleanup functions to provide meaningful diagnostic output when buffer management issues are detected. It constructs a formatted string containing all relevant buffer information that can be logged or displayed for debugging purposes.

## Parameters / Member Variables
- `buffer`: The Buffer identifier for which to generate diagnostic information

## Dependencies
- Functions called/Symbols referenced:
  - BufferDesc (buffer descriptor structure)
  - ProcNumber (process number type)
  - BufferIsLocal (checks if buffer is local)
  - GetLocalBufferDescriptor (gets local buffer descriptor)
  - GetBufferDescriptor (gets shared buffer descriptor)
  - GetPrivateRefCount (gets private reference count)
  - INVALID_PROC_NUMBER (constant for invalid process number)
  - relpathbackend (generates file path for relation)
  - BufTagGetRelFileLocator (extracts file locator from buffer tag)
  - BufTagGetForkNum (extracts fork number from buffer tag)
  - pg_atomic_read_u32 (atomically reads buffer state)
  - BUF_FLAG_MASK (mask for buffer flags)
  - BUF_STATE_GET_REFCOUNT (extracts reference count from state)
- Called from (representative examples):
  - CheckForBufferLeaks
  - ResOwnerPrintBufferPin
  - CheckForLocalBufferLeaks

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller using pfree()
- Handles both shared buffers (positive buffer IDs) and local buffers (negative buffer IDs)
- Provides comprehensive buffer information including buffer ID, file path, block number, flags, and both shared and private reference counts
- Essential debugging tool for diagnosing buffer management issues and leaks
- The function includes a note that theoretically the buffer header should be locked, but this is typically called in debugging contexts where strict locking may not be practical
- Output format: "[buffer_id] (rel=path, blockNum=num, flags=0xX, refcount=shared private)"
- Used extensively throughout the buffer management system for diagnostic and error reporting purposes