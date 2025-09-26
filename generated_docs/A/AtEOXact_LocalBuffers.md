# AtEOXact_LocalBuffers

## Location
src/backend/storage/buffer/localbuf.c: 819 - 829

## Overview
AtEOXact_LocalBuffers performs cleanup operations at the end of a transaction for local buffers, specifically checking for and reporting any local buffer pin leaks that may have occurred during transaction execution.

## Definition
```c
void AtEOXact_LocalBuffers(bool isCommit)
```

## Detailed Description
This function serves as the local buffer equivalent of AtEOXact_Buffers and is called at the end of every transaction (both commit and abort) to ensure proper cleanup of local buffer state. Local buffers are per-backend buffers used for temporary tables and other backend-local storage operations.

The function's primary responsibility is to detect buffer reference count leaks by calling CheckForLocalBufferLeaks(), which verifies that no local buffer pins are still held by the current backend. This is a critical debugging and consistency check that helps maintain buffer pool integrity.

The function is intentionally simple, focusing solely on leak detection rather than performing any actual cleanup operations, as local buffers are expected to be properly unpinned through normal transaction processing.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the transaction is committing (true) or aborting (false). Currently unused by the function but maintained for API consistency with AtEOXact_Buffers.

## Dependencies
- Functions called/Symbols referenced:
  - CheckForLocalBufferLeaks (internal function for detecting buffer reference leaks)
- Called from (representative examples):
  - AtEOXact_Buffers (main buffer cleanup function)
  - ResourceOwnerForgetBufferIO (resource cleanup context)

## Notes and Other Information
- The function is only meaningful in debug builds where USE_ASSERT_CHECKING is defined, as CheckForLocalBufferLeaks() performs its checks only in assertion-enabled builds
- Local buffers are used primarily for temporary tables and are private to each backend process
- Unlike shared buffers, local buffers don't require complex locking mechanisms since they're not shared between processes
- This function complements the shared buffer cleanup performed by AtEOXact_Buffers
- Buffer leaks detected by this function indicate programming errors where buffer pins weren't properly released before transaction end