# _hash_getbuf_with_condlock_cleanup

## Location
[src/backend/access/hash/hashpage.c:96-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L96-L134)

## Overview
Attempts to get a buffer for cleanup operations by trying to acquire a conditional cleanup lock, returning InvalidBuffer if the lock cannot be obtained immediately.

## Definition

```c
Buffer
_hash_getbuf_with_condlock_cleanup(Relation rel, BlockNumber blkno, int flags)
```
## Detailed Description
This function is designed specifically for cleanup operations that should not block. It attempts to acquire a cleanup lock on a hash index page, which is stronger than regular read/write locks and is typically used during maintenance operations like vacuum or page reorganization. The key characteristic is that it uses conditional locking - if the cleanup lock cannot be acquired immediately, the function gives up and returns InvalidBuffer rather than waiting.

The cleanup process follows these steps:
1. Validates that the block number is not P_NEW
2. Reads the buffer from disk
3. Attempts to acquire a cleanup lock conditionally
4. If the lock fails, releases the buffer and returns InvalidBuffer
5. If successful, validates the page and returns the locked buffer

## Parameters / Member Variables
- `rel`: The hash index relation to read from
- `blkno`: Block number of the page to retrieve (must not be P_NEW)
- `flags`: Bitwise OR of allowed page types for validation by _hash_checkpage
## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md) (buffer manager function to read a page)
  - [ConditionalLockBufferForCleanup](../C/ConditionalLockBufferForCleanup.md) (attempts non-blocking cleanup lock)
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (releases buffer if lock fails)
  - [_hash_checkpage](_hash_checkpage.md) (validates page contents and type)
  - P_NEW, InvalidBuffer (constants)
- Called from (representative examples):
  - [_hash_expandtable](_hash_expandtable.md) (during hash table expansion/cleanup)

## Notes and Other Information
- Non-blocking nature makes it suitable for cleanup operations that shouldn't interfere with normal operations
- Cleanup locks are exclusive and stronger than regular read/write locks
- Returns InvalidBuffer if lock cannot be acquired, allowing caller to skip this page
- Used primarily during maintenance operations like hash table expansion
- The conditional locking prevents cleanup operations from becoming bottlenecks

## Simplified Source

```c
Buffer _hash_getbuf_with_condlock_cleanup(Relation rel, BlockNumber blkno, int flags) {
    // P_NEW not allowed - this function only accesses existing pages
    if (blkno == P_NEW) {
        elog(ERROR, "hash AM does not use P_NEW");
    }

    // Read the buffer from disk
    Buffer buf = ReadBuffer(rel, blkno);

    // Try to get cleanup lock without blocking
    if (!ConditionalLockBufferForCleanup(buf)) {
        // Failed to get lock - release buffer and give up
        ReleaseBuffer(buf);
        return InvalidBuffer;
    }

    // Validate page contents and type
    _hash_checkpage(rel, buf, flags);

    return buf;  // Buffer is now cleanup-locked and pinned
}
```