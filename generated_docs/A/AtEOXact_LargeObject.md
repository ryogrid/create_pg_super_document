# AtEOXact_LargeObject

## Location
[src/backend/libpq/be-fsstubs.c:602-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L602-L647)

## Overview
Prepares large objects for transaction commit or abort by cleaning up resources and file descriptors associated with large objects in the current transaction.

## Definition
```c
void AtEOXact_LargeObject(bool isCommit)
```

## Detailed Description
AtEOXact_LargeObject is a critical cleanup function called at the end of transaction processing (both commit and abort). It performs essential resource management for large objects by:

1. Checking if any large object operations occurred during the transaction (`lo_cleanup_needed` flag)
2. On commit: Explicitly closing all open large object file descriptors to avoid resource leak warnings
3. On abort: Skipping the explicit close since the resource owner cleanup will handle it
4. Clearing the cookies array that tracks open large object file descriptors
5. Releasing the large object memory context (`fscxt`) to prevent memory leaks
6. Delegating additional cleanup to the inventory API layer via `close_lo_relation`

The function ensures proper resource cleanup regardless of transaction outcome while optimizing performance by skipping unnecessary operations during abort scenarios.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the transaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [closeLOfd](../c/closeLOfd.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [close_lo_relation](../c/close_lo_relation.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (src/backend/access/transam/xact.c:2271)
  - [PrepareTransaction](../P/PrepareTransaction.md) (src/backend/access/transam/xact.c:2527)
  - [AbortTransaction](AbortTransaction.md) (src/backend/access/transam/xact.c:2859)

## Notes and Other Information
- Only performs cleanup if `lo_cleanup_needed` is true, providing an optimization for transactions that don't use large objects
- On commit, explicitly closes file descriptors to satisfy resource tracking; on abort, relies on automatic cleanup
- Uses a global `cookies` array to track open large object file descriptors
- Memory context deletion ensures no permanent memory leaks from large object operations
- Part of PostgreSQL's two-phase cleanup system where both this function and `close_lo_relation` participate in end-of-transaction processing

## Simplified Source

```c
// Simplified version of AtEOXact_LargeObject
void AtEOXact_LargeObject(bool isCommit) {
    // Skip cleanup if no large object operations occurred
    if (!lo_cleanup_needed)
        return;

    // On commit: explicitly close all open LO file descriptors
    // On abort: skip this since resource owner will clean up automatically
    if (isCommit) {
        for (int i = 0; i < cookies_size; i++) {
            if (cookies[i] != NULL)
                closeLOfd(i);
        }
    }

    // Clear the file descriptor tracking array
    cookies = NULL;
    cookies_size = 0;

    // Release large object memory context to prevent leaks
    if (fscxt)
        MemoryContextDelete(fscxt);
    fscxt = NULL;

    // Let inventory API do additional cleanup
    close_lo_relation(isCommit);

    // Mark cleanup as complete
    lo_cleanup_needed = false;
}
```

Key simplifications made:
- Removed detailed comments about memory context lifecycle
- Simplified variable declarations (moved loop variable inline)
- Added high-level comments explaining the logic flow
- Consolidated the main cleanup steps into clear sections
- Preserved all essential functionality and error handling