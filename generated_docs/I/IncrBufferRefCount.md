# IncrBufferRefCount

## Location
src/backend/storage/buffer/bufmgr.c: 4929 - 4960

## Overview
IncrBufferRefCount increments the pin count on a buffer that is already pinned at least once, without changing the shared buffer state.

## Definition


## Detailed Description
This function provides a way to increment the reference count (pin count) on a buffer that the current process has already pinned. Unlike the initial buffer pin operations, this function only modifies the local reference count and does not interact with the shared buffer state, making it more efficient for scenarios where multiple references to the same buffer are needed within a single process. The function handles both local buffers (temporary tables) and shared buffers differently, maintaining separate reference counting mechanisms for each type.

## Parameters / Member Variables
- : The Buffer identifier for an already-pinned buffer whose reference count should be incremented

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsPinned (assertion check)
  - ResourceOwnerEnlarge
  - BufferIsLocal
  - GetPrivateRefCountEntry
  - ResourceOwnerRememberBuffer
  - PrivateRefCountEntry (type)
- Called from (representative examples):
  - scanPostingTree (GIN index scanning)
  - startScanEntry (GIN index operations)
  - ReadBufferBI (heap I/O operations)
  - btrestrpos (B-tree positioning)
  - _bt_steppage (B-tree page navigation)

## Notes and Other Information
- Can only be used on buffers that are already pinned (enforced by assertion)
- Does not modify shared buffer state, only local reference counts
- Handles local buffers (negative buffer IDs) and shared buffers differently
- Integrates with PostgreSQL's resource owner system for proper cleanup tracking
- Essential for scenarios where code needs multiple references to the same buffer
- More efficient than acquiring a new pin since it bypasses shared state modifications