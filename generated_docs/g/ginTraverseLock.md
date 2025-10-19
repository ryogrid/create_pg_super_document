# ginTraverseLock

## Location
[src/backend/access/gin/ginbtree.c:39-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginbtree.c#L39-L82)

## Overview
ginTraverseLock locks a buffer using the appropriate method for GIN B-tree search operations, handling dynamic lock upgrades when necessary.

## Definition
int ginTraverseLock(Buffer buffer, bool searchMode)

## Detailed Description
ginTraverseLock is a critical function in PostgreSQL's GIN (Generalized Inverted Index) implementation that manages buffer locking during tree traversal. The function initially acquires a shared lock on the buffer, then examines the page to determine if lock escalation is necessary. For leaf pages in non-search mode (typically insertion operations), it upgrades to an exclusive lock to allow modifications. The function handles the rare case where a root page becomes non-leaf during relock by reverting to the original shared lock.

## Parameters / Member Variables
- : The buffer to be locked, containing a GIN B-tree page
- : Boolean flag indicating the operation type - true for search operations (read-only), false for modification operations

## Dependencies
- Functions called/Symbols referenced:
  - [LockBuffer](../L/LockBuffer.md) (buffer locking operations)
  - [BufferGetPage](../B/BufferGetPage.md) (page retrieval from buffer)
  - GinPageIsLeaf (page type checking)
  - GIN_SHARE, GIN_EXCLUSIVE, GIN_UNLOCK (lock type constants)
- Called from (representative examples):
  - [ginFindLeafPage](ginFindLeafPage.md)
  - [ginCompareItemPointers](ginCompareItemPointers.md)

## Notes and Other Information
The function implements a sophisticated locking strategy that balances concurrency with data integrity. The relock mechanism handles the edge case where page structure changes during lock upgrade, which can occur in high-concurrency scenarios. The returned access value indicates the final lock type held, which callers use to determine subsequent unlock behavior.

## Simplified Source

```c
int ginTraverseLock(Buffer buffer, bool searchMode) {
    int access = GIN_SHARE;

    // Start with shared lock
    LockBuffer(buffer, GIN_SHARE);
    Page page = BufferGetPage(buffer);

    // Check if we need exclusive access for modifications
    if (GinPageIsLeaf(page) && !searchMode) {
        // Upgrade to exclusive lock for leaf modifications
        LockBuffer(buffer, GIN_UNLOCK);
        LockBuffer(buffer, GIN_EXCLUSIVE);

        // Handle rare case: page became non-leaf during relock
        if (!GinPageIsLeaf(page)) {
            // Revert to shared lock
            LockBuffer(buffer, GIN_UNLOCK);
            LockBuffer(buffer, GIN_SHARE);
        } else {
            access = GIN_EXCLUSIVE;
        }
    }

    return access;
}
```