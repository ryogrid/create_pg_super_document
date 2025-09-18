# RT_ATTACH

## Location
[src/include/lib/radixtree.h:1899-1925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1899-L1925)

## Overview
RT_ATTACH attaches to an existing shared memory radix tree using a DSA area and handle, creating a local tree structure that can access the shared tree.

## Definition
```c
RT_SCOPE RT_RADIX_TREE *RT_ATTACH(dsa_area *dsa, RT_HANDLE handle)
```

## Detailed Description
This function attaches a backend process to an existing radix tree stored in shared memory through a Dynamic Shared Area (DSA). It creates a new local RT_RADIX_TREE structure that serves as an interface to the shared tree control structure. The function allocates the tree structure in the current memory context, locates the shared control object using the provided handle, and initializes an iteration context for tree traversal operations. This allows multiple PostgreSQL backend processes to share access to the same radix tree data structure while maintaining proper memory context isolation.

## Parameters / Member Variables
- `dsa`: Pointer to the Dynamic Shared Area containing the shared radix tree
- `handle`: Handle (dsa_pointer typedef) identifying the specific radix tree control structure in shared memory

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - CurrentMemoryContext
  - [dsa_get_address](../d/dsa_get_address.md)
  - AllocSetContextCreate
  - RT_STR
  - RT_PREFIX
  - ALLOCSET_SMALL_SIZES
  - RT_RADIX_TREE_MAGIC
- Called from (representative examples):
  - RT_HANDLE (as referenced)

## Notes and Other Information
- Only available when RT_SHMEM is defined (shared memory radix trees)
- Creates iteration context for backend-specific tree traversal operations
- The returned tree structure is allocated in the current memory context
- Requires the shared tree control structure to have valid magic number
- Essential for multi-process access to shared radix tree data structures
- Must be paired with RT_DETACH when the attachment is no longer needed