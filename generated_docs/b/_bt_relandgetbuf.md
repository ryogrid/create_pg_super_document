# _bt_relandgetbuf

## Location
src/backend/access/nbtree/nbtpage.c: 1003 - 1022

## Overview
_bt_relandgetbuf releases a currently held buffer and acquires a new buffer in a single operation, serving as a convenient combination of _bt_relbuf followed by _bt_getbuf.

## Definition
```c
Buffer _bt_relandgetbuf(Relation rel, Buffer obuf, BlockNumber blkno, int access)
```

## Detailed Description
This function provides an efficient way to switch from one buffer to another by combining the release and acquisition operations. While originally motivated to reduce buffer manager overhead by avoiding two separate calls, it now primarily serves as a notational convenience that simplifies caller code.

The function handles the special case where obuf is InvalidBuffer, in which case it simply reduces to _bt_getbuf. It also optimizes for the case where the target page is the same as the one already in the buffer, saving unnecessary work. The operation maintains proper locking semantics by unlocking the old buffer before acquiring the new one, then applying the appropriate lock and performing sanity checks.

## Parameters / Member Variables
- `rel`: The relation containing the target block
- `obuf`: The buffer currently held (can be InvalidBuffer)
- `blkno`: The block number of the target page to acquire
- `access`: The type of access required for the new buffer (read or write)

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumberIsValid (validates the target block number)
  - [BufferIsValid](../B/BufferIsValid.md) (checks if old buffer is valid)
  - [_bt_unlockbuf](_bt_unlockbuf.md) (releases lock on old buffer)
  - ReleaseAndReadBuffer (core buffer manager function for release+read)
  - [_bt_lockbuf](_bt_lockbuf.md) (applies appropriate lock to new buffer)
  - [_bt_checkpage](_bt_checkpage.md) (performs sanity checks on the new page)
- Called from (representative examples):
  - [_bt_check_unique](_bt_check_unique.md) (during uniqueness checking)
  - [_bt_stepright](_bt_stepright.md) (when moving right during insertions)
  - [_bt_search](_bt_search.md) (during B-tree traversal)
  - [_bt_moveright](_bt_moveright.md) (when navigating rightward)
  - [_bt_get_endpoint](_bt_get_endpoint.md) (when finding tree endpoints)

## Notes and Other Information
- Combines buffer release and acquisition into a single atomic operation
- Handles InvalidBuffer case gracefully by reducing to simple _bt_getbuf
- Optimizes for same-page case where target is already in the buffer
- Maintains proper unlock-then-lock semantics to avoid deadlocks
- Primarily serves as a notational convenience rather than performance optimization
- Returns a locked and pinned buffer ready for safe page access
- Located in src/backend/access/nbtree/nbtpage.c:1003-1022