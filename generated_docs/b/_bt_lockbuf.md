# _bt_lockbuf

## Location
[src/backend/access/nbtree/nbtpage.c:1039-1069](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1039-L1069)

## Overview
_bt_lockbuf locks a pinned buffer for safe page access, serving as a wrapper around LockBuffer() with additional Valgrind support for memory debugging.

## Definition
```c
void _bt_lockbuf(Relation rel, Buffer buf, int access)
```

## Detailed Description
This function provides the standard way to lock an already-pinned buffer in the B-tree system. It acts as a wrapper around the raw LockBuffer() call but includes additional steps needed for proper Valgrind integration. The function ensures that pages become accessible to Valgrind when locked, helping detect unsafe memory accesses during debugging.

The function includes comprehensive comments about Valgrind behavior and memory safety considerations. It explains how IndexTuple C pointers computed from pages become unsafe to dereference once the lock is released, and how Valgrind can help detect such unsafe accesses. The function also handles the interaction between nbtree client requests and buffer manager pin requests.

## Parameters / Member Variables
- `rel`: The relation associated with the buffer (used to determine if local buffers are used)
- `buf`: The buffer to lock (must already be pinned by the calling backend)
- `access`: The type of lock to acquire (BT_READ or BT_WRITE)

## Dependencies
- Functions called/Symbols referenced:
  - [LockBuffer](../L/LockBuffer.md) (core buffer manager locking function)
  - RelationUsesLocalBuffers (checks if relation uses local buffer management)
  - VALGRIND_MAKE_MEM_DEFINED (makes buffer memory accessible to Valgrind)
- Called from (representative examples):
  - [_bt_getbuf](_bt_getbuf.md) (after reading a buffer)
  - [_bt_relandgetbuf](_bt_relandgetbuf.md) (when switching buffers)
  - [_bt_getroot](_bt_getroot.md) (when accessing root page)
  - [_bt_search](_bt_search.md) (during tree traversal)
  - [_bt_moveright](_bt_moveright.md) (during rightward navigation)

## Notes and Other Information
- Essential wrapper around LockBuffer() that enforces proper nbtree locking conventions
- Includes sophisticated Valgrind integration for memory debugging support
- Caller must ensure buffer is already pinned before calling this function
- Contains detailed comments about memory safety and pointer validity after lock release
- Handles both local and shared buffer pools appropriately
- Error conditions don't cause Valgrind false positives due to careful design
- Located in src/backend/access/nbtree/nbtpage.c:1039-1069

## Simplified Source

```c
void
_bt_lockbuf(Relation rel, Buffer buf, int access)
{
    // Lock the buffer (must already be pinned)
    LockBuffer(buf, access);

    // Make buffer memory accessible to Valgrind for debugging
    if (!RelationUsesLocalBuffers(rel)) {
        VALGRIND_MAKE_MEM_DEFINED(BufferGetPage(buf), BLCKSZ);
    }
}
```