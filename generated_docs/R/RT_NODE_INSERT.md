# RT_NODE_INSERT

## Location
src/include/lib/radixtree.h: 1541 - 1580

## Overview
A macro that generates the function name for inserting a child into a radix tree node, serving as the main dispatcher for node-type-specific insertion operations in PostgreSQL's adaptive radix tree.

## Definition
```c
#define RT_NODE_INSERT RT_MAKE_NAME(node_insert)
static inline RT_PTR_ALLOC *
RT_NODE_INSERT(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk)
```

## Detailed Description
RT_NODE_INSERT is both a macro that expands to a function name and the central insertion function for radix tree nodes. This function serves as a dispatcher that examines the node type and either adds a child directly (if there's capacity) or triggers a node growth operation (if the node is at capacity).

The function implements the adaptive behavior of the radix tree by handling all four node types (node-4, node-16, node-48, node-256) with their respective capacity limits and growth patterns. For each node type, it first checks if growth is needed using RT_NODE_MUST_GROW, and if so, calls the appropriate growth function. Otherwise, it calls the appropriate add_child function.

This is a critical function in the radix tree's insertion path, called when a slot for a new child needs to be created during tree traversal.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent node's slot that references this node (used for growth operations)
- `node`: The radix tree node where the child will be inserted
- `chunk`: The key chunk (8-bit value) that will index the new child

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_NODE_MUST_GROW (capacity check)
  - RT_GROW_NODE_4, RT_GROW_NODE_16, RT_GROW_NODE_48 (growth operations)
  - RT_ADD_CHILD_4, RT_ADD_CHILD_16, RT_ADD_CHILD_48, RT_ADD_CHILD_256 (insertion operations)
- Called from (representative examples):
  - RT_GET_SLOT_RECURSIVE (when creating slots during tree traversal)

## Notes and Other Information
- Central dispatcher for all node insertion operations in the adaptive radix tree
- Implements the switch statement that handles all four node kinds (4, 16, 48, 256)
- Node-256 never needs to grow since it can accommodate all possible 8-bit chunk values
- The function returns a pointer to the newly allocated slot where the caller can store the child
- Growth operations handle both expanding the node and inserting the new child
- Critical for maintaining the adaptive nature of the tree by triggering growth when needed
- Uses `unlikely()` hint for growth conditions since most insertions don't require growth
- Part of the inline fast path for radix tree operations