# XLogPrefetcherGetReader

## Location
src/backend/access/transam/xlogprefetcher.c: 403 - 411

## Overview
A simple accessor function that returns the XLogReaderState associated with a WAL prefetcher instance.

## Definition
```c
XLogReaderState *XLogPrefetcherGetReader(XLogPrefetcher *prefetcher)
```

## Detailed Description
XLogPrefetcherGetReader provides controlled access to the XLogReaderState that was associated with the prefetcher during its allocation. This function serves as a clean interface for retrieving the WAL reader, maintaining encapsulation of the prefetcher's internal structure while allowing necessary access to the reader for WAL processing operations.

The function is straightforward, simply returning the reader pointer that was stored during prefetcher initialization.

## Parameters / Member Variables
- `prefetcher`: Pointer to the XLogPrefetcher instance from which to retrieve the reader

## Dependencies
- Functions called/Symbols referenced:
  - None (direct member access)
- Called from (representative examples):
  - [ReadRecord](../R/ReadRecord.md) (during WAL record reading operations)

## Notes and Other Information
- This is a simple getter function that maintains encapsulation by providing controlled access to internal prefetcher state
- The returned XLogReaderState pointer is the same one that was passed to XLogPrefetcherAllocate during initialization
- Used by WAL recovery code when it needs direct access to the reader state for processing records
- The function assumes the prefetcher parameter is valid and non-NULL (no defensive checks are performed)