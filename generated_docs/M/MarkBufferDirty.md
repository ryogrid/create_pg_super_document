# MarkBufferDirty

## Location
src/backend/storage/buffer/bufmgr.c: 2520 - 2582

## Overview
MarkBufferDirty marks a buffer's contents as modified (dirty), indicating that the data needs to be written to disk during the next checkpoint or buffer eviction, and updates vacuum accounting statistics.

## Definition
```c
void MarkBufferDirty(Buffer buffer)
```

## Detailed Description
This function sets the dirty flag on a buffer to indicate that its contents have been modified and need to be written to storage. The function handles both shared buffers (managed by the buffer manager) and local buffers (used for temporary tables) differently. For shared buffers, it uses atomic compare-and-swap operations to safely update the buffer state flags even in a multi-threaded environment.

The function implements a retry loop with atomic operations to handle concurrent access. It sets both the BM_DIRTY flag (indicating the buffer is dirty) and BM_JUST_DIRTIED flag (for timing-related optimizations). When a buffer transitions from clean to dirty, it updates vacuum accounting statistics including the global dirty page count and vacuum cost accounting.

## Parameters / Member Variables
- `buffer`: Buffer identifier to mark as dirty. Can be either a shared buffer (positive value) or local buffer (negative value)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsValid: Validates buffer identifier
  - BufferIsLocal: Determines if buffer is a local buffer
  - MarkLocalBufferDirty: Handles marking local buffers dirty
  - GetBufferDescriptor: Gets descriptor for shared buffers
  - BufferIsPinned: Assertion to verify buffer is pinned
  - LWLockHeldByMeInMode: Assertion to verify exclusive lock is held
  - BufferDescriptorGetContentLock: Gets the content lock for the buffer
  - pg_atomic_read_u32: Atomic read of buffer state
  - WaitBufHdrUnlocked: Waits for buffer header to be unlocked
  - BUF_STATE_GET_REFCOUNT: Extracts reference count from buffer state
  - pg_atomic_compare_exchange_u32: Atomic compare-and-swap operation
  - BM_DIRTY: Buffer state flag indicating dirty status
  - BM_JUST_DIRTIED: Buffer state flag for recent dirty marking
  - BM_LOCKED: Buffer state flag indicating locked status
- Called from (representative examples):
  - No direct references found (likely called through macros or inline functions)

## Notes and Other Information
- The buffer must be pinned before calling this function to prevent eviction
- An exclusive content lock must be held to ensure safe modification of buffer contents
- Uses atomic operations and retry loops to handle concurrent access safely
- Local buffers are handled separately through MarkLocalBufferDirty()
- Updates vacuum accounting statistics when a buffer transitions from clean to dirty
- The BM_JUST_DIRTIED flag is used for performance optimizations in checkpoint timing
- VacuumPageDirty and pgBufferUsage.shared_blks_dirtied counters are incremented for newly dirty buffers
- Integrates with vacuum cost accounting system when VacuumCostActive is enabled