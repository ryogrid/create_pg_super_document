# RT_NODE_DELETE

## Location
src/include/lib/radixtree.h: 2576 - 2606

## Overview
RT_NODE_DELETE is a macro that expands to a function serving as the central dispatcher for deleting child pointers from internal nodes in PostgreSQL's radix tree implementation.

## Definition
```c
#define RT_NODE_DELETE RT_MAKE_NAME(node_delete)

static inline void
RT_NODE_DELETE(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk, RT_PTR_ALLOC *slot)
```

## Detailed Description
This function acts as a polymorphic dispatcher that routes deletion operations to the appropriate node-type-specific removal function based on the node's kind. It implements a classic switch-case pattern to handle the different internal node types in the radix tree hierarchy:

- **RT_NODE_KIND_4**: Routes to RT_REMOVE_CHILD_4 for smallest nodes
- **RT_NODE_KIND_16**: Routes to RT_REMOVE_CHILD_16 for small-to-medium nodes  
- **RT_NODE_KIND_48**: Routes to RT_REMOVE_CHILD_48 for medium-to-large nodes
- **RT_NODE_KIND_256**: Routes to RT_REMOVE_CHILD_256 for largest nodes

The function abstracts away the specific implementation details of each node type, providing a uniform interface for node deletion operations throughout the radix tree code. This design enables polymorphic behavior while maintaining type safety and performance.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that contains this node (for potential updates)
- `node`: The node from which to remove the child (determines which removal function to call)
- `chunk`: The 8-bit chunk value identifying which child to remove
- `slot`: Pointer to the exact slot in the children array to be removed (not used for node48/256)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - [RT_REMOVE_CHILD_4](RT_REMOVE_CHILD_4.md)
  - [RT_REMOVE_CHILD_16](RT_REMOVE_CHILD_16.md)
  - [RT_REMOVE_CHILD_48](RT_REMOVE_CHILD_48.md)
  - [RT_REMOVE_CHILD_256](RT_REMOVE_CHILD_256.md)
- Called from (representative examples):
  - [RT_DELETE_RECURSIVE](RT_DELETE_RECURSIVE.md)

## Notes and Other Information
- This function is marked as static inline for performance optimization
- Uses a switch statement with pg_unreachable() for the default case to ensure all node types are handled
- The slot parameter is not used for node48 and node256 operations since those node types use different indexing mechanisms
- Part of the recursive deletion strategy where RT_DELETE_RECURSIVE calls this function at appropriate levels
- Provides the abstraction layer that enables PostgreSQL's generic radix tree to work with different node sizes efficiently
- Called in two contexts: direct leaf deletion (shift == 0) and cascade deletion when child nodes are freed
- Located in src/include/lib/radixtree.h at lines 2575-2603