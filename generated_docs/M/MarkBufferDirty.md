# MarkBufferDirty

## Location
[src/backend/storage/buffer/bufmgr.c:2520-2582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2520-L2582)

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
  - [BufferIsValid](../B/BufferIsValid.md): Validates buffer identifier
  - BufferIsLocal: Determines if buffer is a local buffer
  - [MarkLocalBufferDirty](MarkLocalBufferDirty.md): Handles marking local buffers dirty
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md): Gets descriptor for shared buffers
  - BufferIsPinned: Assertion to verify buffer is pinned
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md): Assertion to verify exclusive lock is held
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md): Gets the content lock for the buffer
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md): Atomic read of buffer state
  - [WaitBufHdrUnlocked](../W/WaitBufHdrUnlocked.md): Waits for buffer header to be unlocked
  - BUF_STATE_GET_REFCOUNT: Extracts reference count from buffer state
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md): Atomic compare-and-swap operation
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

## Simplified Source

```c
void MarkBufferDirty(Buffer buffer) {
    // Validate buffer and handle local buffers separately
    if (!BufferIsValid(buffer))
        elog(ERROR, "bad buffer ID: %d", buffer);

    if (BufferIsLocal(buffer)) {
        MarkLocalBufferDirty(buffer);
        return;
    }

    // Get buffer descriptor for shared buffer
    BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);

    // Verify buffer is pinned and exclusively locked
    Assert(BufferIsPinned(buffer));
    Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE));

    // Atomically set dirty flags using compare-and-swap loop
    uint32 old_buf_state = pg_atomic_read_u32(&bufHdr->state);
    for (;;) {
        // Wait if buffer header is locked
        if (old_buf_state & BM_LOCKED)
            old_buf_state = WaitBufHdrUnlocked(bufHdr);

        // Set dirty and just-dirtied flags
        uint32 buf_state = old_buf_state | BM_DIRTY | BM_JUST_DIRTIED;

        // Try to update atomically; retry if state changed concurrently
        if (pg_atomic_compare_exchange_u32(&bufHdr->state, &old_buf_state, buf_state))
            break;
    }

    // Update vacuum accounting if buffer wasn't already dirty
    if (!(old_buf_state & BM_DIRTY)) {
        VacuumPageDirty++;
        pgBufferUsage.shared_blks_dirtied++;
        if (VacuumCostActive)
            VacuumCostBalance += VacuumCostPageDirty;
    }
}
```

**Key Logic:**
- Validates buffer ID and delegates local buffers to specialized handler
- Atomically sets BM_DIRTY and BM_JUST_DIRTIED flags using compare-and-swap
- Handles concurrent access through retry loop and buffer header locking
- Updates vacuum statistics only when buffer transitions from clean to dirty
- Requires buffer to be pinned and exclusively locked for safe modification