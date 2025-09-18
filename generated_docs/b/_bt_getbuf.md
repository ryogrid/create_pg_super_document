# _bt_getbuf

## Location
[src/backend/access/nbtree/nbtpage.c:845-868](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L845-L868)

## Overview
_bt_getbuf is a core B-tree buffer management function that retrieves an existing block from a B-tree relation and ensures it is properly locked and pinned for safe access.

## Definition


## Detailed Description
This function implements the fundamental rule of nbtree buffer management: it's never okay to access a page without holding both a buffer pin and a buffer lock on the page's buffer. The function reads an existing block from the relation, applies the appropriate lock based on the access parameter, and performs sanity checks on the page. It serves as a safe wrapper around ReadBuffer() that ensures proper locking semantics are maintained.

The function also applies _bt_checkpage to sanity-check the page and performs Valgrind client requests that help detect unsafe page accesses. All buffer lock requests in nbtree must go through wrapper functions like this one rather than calling LockBuffer() directly.

## Parameters / Member Variables
- : The relation (B-tree index) from which to read the block
- : The block number to retrieve (must be a valid block number)
- : The type of access required (read or write), passed to _bt_lockbuf for appropriate locking

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumberIsValid (validates the block number)
  - [ReadBuffer](../R/ReadBuffer.md) (reads the block into buffer pool)
  - [_bt_lockbuf](_bt_lockbuf.md) (applies appropriate buffer lock)
  - [_bt_checkpage](_bt_checkpage.md) (performs sanity checks on the page)
- Called from (representative examples):
  - [_bt_insertonpg](_bt_insertonpg.md) (during B-tree insertion)
  - [_bt_split](_bt_split.md) (during page splitting operations)
  - [_bt_getroot](_bt_getroot.md) (when accessing the root page)
  - [_bt_moveright](_bt_moveright.md) (during rightward page navigation)
  - [_bt_pagedel](_bt_pagedel.md) (during page deletion)

## Notes and Other Information
- This is a fundamental building block for all B-tree page access operations
- The function enforces the critical nbtree invariant that pages must always be both pinned and locked
- Raw LockBuffer() calls are disallowed in nbtree code - all locking must go through wrapper functions
- The returned buffer is both locked and pinned, ready for safe page access
- Located in src/backend/access/nbtree/nbtpage.c:845-868