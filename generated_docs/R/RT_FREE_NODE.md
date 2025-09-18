# RT_FREE_NODE

## Location
[src/include/lib/radixtree.h:927-958](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L927-L958)

## Overview
RT_FREE_NODE is a macro that resolves to a function for deallocating radix tree internal nodes and updating debug statistics.

## Definition
```c
#define RT_FREE_NODE RT_MAKE_NAME(free_node)

// The actual function signature:
static void RT_FREE_NODE(RT_RADIX_TREE * tree, RT_CHILD_PTR node)
```

## Detailed Description
RT_FREE_NODE is part of PostgreSQL's templated radix tree implementation that handles the deallocation of internal nodes. The function performs proper cleanup by updating debug statistics (when RT_DEBUG is enabled) and freeing the allocated memory. It includes special handling for node256 types where the fanout field overflows to zero in the header, requiring additional logic to correctly identify the size class for statistics tracking. The memory deallocation strategy varies based on whether shared memory (RT_SHMEM) or regular memory contexts are being used.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure that contains the node to be freed
- `node`: RT_CHILD_PTR structure representing the node to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - [dsa_free](../d/dsa_free.md) (for shared memory deallocation when RT_SHMEM is defined)
  - [pfree](../p/pfree.md) (for regular memory deallocation)
  - Assert (for debug assertions)
  - RT_SIZE_CLASS_INFO array access
- Called from (representative examples):
  - [RT_GROW_NODE_48](RT_GROW_NODE_48.md) (after growing node48 to node256)
  - [RT_GROW_NODE_16](RT_GROW_NODE_16.md) (after growing node16 to larger sizes)
  - [RT_GROW_NODE_4](RT_GROW_NODE_4.md) (after growing node4 to node16)
  - [RT_SHRINK_NODE_256](RT_SHRINK_NODE_256.md) (after shrinking node256 to node48)
  - [RT_SHRINK_NODE_48](RT_SHRINK_NODE_48.md) (after shrinking node48 to node16)
  - [RT_SHRINK_NODE_16](RT_SHRINK_NODE_16.md) (after shrinking node16 to node4)
  - [RT_REMOVE_CHILD_4](RT_REMOVE_CHILD_4.md) (when removing the last child from node4)

## Notes and Other Information
- Updates debug statistics by decrementing the node count for the appropriate size class when RT_DEBUG is enabled
- Includes special logic to handle node256 fanout overflow (appears as 0 in header due to uint8 overflow)
- Memory deallocation method depends on RT_SHMEM compilation flag (shared vs local memory)
- Critical for preventing memory leaks during node restructuring operations
- Used extensively during tree growth, shrinking, and deletion operations
- The function does not recursively free child nodes - caller must handle that separately if needed