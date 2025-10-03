# _bt_unlockbuf

## Location
[src/backend/access/nbtree/nbtpage.c:1070-1092](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1070-L1092)

## Overview
_bt_unlockbuf unlocks a pinned B-tree buffer while maintaining proper memory access patterns and validation checks for debugging.

## Definition

```c
void
_bt_unlockbuf(Relation rel, Buffer buf)
```
## Detailed Description
_bt_unlockbuf is a fundamental B-tree buffer management function that safely unlocks a previously locked buffer. The function performs memory validation checks using Valgrind instrumentation to ensure the buffer's memory is properly defined and accessible before unlocking. After unlocking the buffer using LockBuffer with BUFFER_LOCK_UNLOCK mode, it conditionally marks the buffer memory as inaccessible for non-local relations to help catch use-after-unlock bugs in debug builds.

This function is part of PostgreSQL's careful buffer management protocol where buffers must be both pinned (to prevent eviction) and locked (for exclusive access) during critical operations. The function specifically handles the unlocking phase while the buffer remains pinned.

## Parameters / Member Variables
- `rel`: The relation (table/index) that owns the buffer
- `buf`: The buffer descriptor for the page to be unlocked
## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_CHECK_MEM_IS_DEFINED
  - [BufferGetPage](../B/BufferGetPage.md)  
  - [LockBuffer](../L/LockBuffer.md)
  - BUFFER_LOCK_UNLOCK
  - RelationUsesLocalBuffers
  - VALGRIND_MAKE_MEM_NOACCESS

- Called from (representative examples):
  - [_bt_getroot](_bt_getroot.md)
  - [_bt_relandgetbuf](_bt_relandgetbuf.md)
  - [_bt_relbuf](_bt_relbuf.md)
  - [_bt_pagedel](_bt_pagedel.md)
  - [_bt_search](_bt_search.md)
  - [_bt_moveright](_bt_moveright.md)
  - [_bt_first](_bt_first.md)
  - [_bt_killitems](_bt_killitems.md)

## Notes and Other Information
- The function includes Valgrind memory checking instrumentation for debugging buffer access patterns
- Only marks memory as inaccessible for shared buffers (non-local relations) to avoid interfering with local buffer usage
- Buffer remains pinned after this call - unpinning requires a separate _bt_relbuf call
- Critical for maintaining B-tree concurrency control and preventing buffer access races
- The function assumes the buffer is already both pinned and locked by the calling backend

## Simplified Source

```c
void
_bt_unlockbuf(Relation rel, Buffer buf)
{
    // Validate that buffer memory is properly defined before unlocking
    VALGRIND_CHECK_MEM_IS_DEFINED(BufferGetPage(buf), BLCKSZ);

    // Unlock the buffer
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);

    // Mark buffer memory as inaccessible for shared buffers to catch use-after-unlock
    if (!RelationUsesLocalBuffers(rel)) {
        VALGRIND_MAKE_MEM_NOACCESS(BufferGetPage(buf), BLCKSZ);
    }
}
```