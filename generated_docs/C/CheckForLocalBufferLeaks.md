# CheckForLocalBufferLeaks

## Location
[src/backend/storage/buffer/localbuf.c:786-818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L786-L818)

## Overview
CheckForLocalBufferLeaks is a debugging function that detects and reports local buffer reference count leaks during assert checking builds.

## Definition
```c
static void CheckForLocalBufferLeaks(void)
```

## Detailed Description
CheckForLocalBufferLeaks is a diagnostic function that helps identify resource leaks in local buffer management. The function operates only in debug builds (when USE_ASSERT_CHECKING is defined) and scans through all local buffer reference counts to detect any buffers that still have non-zero reference counts when they should have been unpinned.

When a leak is detected, the function generates detailed warning messages using DebugPrintBufferRefcount to provide information about the leaked buffer. This helps developers identify and fix bugs where buffers are pinned but never properly unpinned, which could lead to resource exhaustion over time.

The function is similar in purpose to CheckForBufferLeaks() but specifically targets the local buffer subsystem. It's designed to be called during cleanup operations (end of transaction, process exit) to ensure that all local buffer pins have been properly released.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - DebugPrintBufferRefcount (for diagnostic output)
  - elog (for warning messages)
  - pfree (for memory cleanup)
- Global variables accessed:
  - LocalRefCount (array of reference counts for local buffers)
  - NLocBuffer (total number of local buffers)
- Called from (representative examples):
  - AtEOXact_LocalBuffers (at end of transaction)
  - AtProcExit_LocalBuffers (at process exit)

## Notes and Other Information
- The function is static, meaning it's only accessible within the localbuf.c file
- Only compiled and executed in debug builds (USE_ASSERT_CHECKING must be defined)
- Helps detect buffer pin/unpin imbalances that could indicate programming errors
- The function converts buffer indices back to buffer identifiers using the formula: b = -i - 1
- Uses assertions to abort execution if leaks are detected, helping catch bugs during development
- Part of PostgreSQL's comprehensive resource leak detection infrastructure
- The function complements the shared buffer leak detection provided by CheckForBufferLeaks()
- Warning messages provide detailed information about leaked buffers to aid in debugging