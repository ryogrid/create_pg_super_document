# RT_DETACH

## Location
[src/include/lib/radixtree.h:1926-1933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1926-L1933)

## Overview
RT_DETACH detaches from a shared memory radix tree by cleaning up the local tree structure and associated memory contexts.

## Definition
```c
RT_SCOPE void RT_DETACH(RT_RADIX_TREE * tree)
```

## Detailed Description
This function detaches a backend process from a shared memory radix tree by properly cleaning up the local tree structure that was created by RT_ATTACH. It verifies that the tree control structure is valid by checking the magic number, deletes the iteration memory context that was allocated during attachment, and frees the local tree structure. This function is essential for proper resource cleanup when a backend no longer needs access to a shared radix tree, preventing memory leaks and ensuring clean disconnection from shared memory structures.

## Parameters / Member Variables
- `tree`: Pointer to the local radix tree structure to detach and clean up

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [pfree](../p/pfree.md)
  - RT_RADIX_TREE_MAGIC
- Called from (representative examples):
  - RT_HANDLE (as referenced)

## Notes and Other Information
- Only available when RT_SHMEM is defined (shared memory radix trees)
- Must be called to properly clean up resources after RT_ATTACH
- Validates tree integrity using magic number before cleanup
- Deletes the iteration context created during attachment
- Does not affect the actual shared tree data, only the local attachment
- Essential for preventing memory leaks in multi-process radix tree usage
- Part of the shared memory radix tree lifecycle management