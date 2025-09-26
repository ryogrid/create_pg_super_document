# IncrBufferRefCount

## Location
[src/backend/storage/buffer/bufmgr.c:4929-4960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4929-L4960)

## Overview
IncrBufferRefCount increments the pin count on a buffer that is already pinned at least once, without changing the shared buffer state.

## Definition

```c
void
IncrBufferRefCount(Buffer buffer)
```
## Detailed Description
This function provides a way to increment the reference count (pin count) on a buffer that the current process has already pinned. Unlike the initial buffer pin operations, this function only modifies the local reference count and does not interact with the shared buffer state, making it more efficient for scenarios where multiple references to the same buffer are needed within a single process. The function handles both local buffers (temporary tables) and shared buffers differently, maintaining separate reference counting mechanisms for each type.

## Parameters / Member Variables
- : The Buffer identifier for an already-pinned buffer whose reference count should be incremented

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsPinned (assertion check)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - BufferIsLocal
  - [GetPrivateRefCountEntry](../G/GetPrivateRefCountEntry.md)
  - [ResourceOwnerRememberBuffer](../R/ResourceOwnerRememberBuffer.md)
  - [PrivateRefCountEntry](../P/PrivateRefCountEntry.md) (type)
- Called from (representative examples):
  - [scanPostingTree](../s/scanPostingTree.md) (GIN index scanning)
  - [startScanEntry](../s/startScanEntry.md) (GIN index operations)
  - [ReadBufferBI](../R/ReadBufferBI.md) (heap I/O operations)
  - [btrestrpos](../b/btrestrpos.md) (B-tree positioning)
  - [_bt_steppage](../b/_bt_steppage.md) (B-tree page navigation)

## Notes and Other Information
- Can only be used on buffers that are already pinned (enforced by assertion)
- Does not modify shared buffer state, only local reference counts
- Handles local buffers (negative buffer IDs) and shared buffers differently
- Integrates with PostgreSQL's resource owner system for proper cleanup tracking
- Essential for scenarios where code needs multiple references to the same buffer
- More efficient than acquiring a new pin since it bypasses shared state modifications