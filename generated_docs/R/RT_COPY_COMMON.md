# RT_COPY_COMMON

## Location
[src/include/lib/radixtree.h:920-926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L920-L926)

## Overview
RT_COPY_COMMON is a macro that resolves to a function for copying common header fields from one radix tree node to another during node operations.

## Definition
```c
#define RT_COPY_COMMON RT_MAKE_NAME(copy_common)

// The actual function signature:
static inline void RT_COPY_COMMON(RT_CHILD_PTR newnode, RT_CHILD_PTR oldnode)
```

## Detailed Description
RT_COPY_COMMON is part of PostgreSQL's templated radix tree implementation. This function copies relevant members of the node header from an old node to a new node. Currently, it only copies the count field, which tracks the number of child entries in the node. The function is designed as a separate utility to facilitate future extensibility - if additional common fields need to be copied between nodes, they can be added to this function without modifying multiple call sites.

## Parameters / Member Variables
- `newnode`: Destination node that will receive the copied header information
- `oldnode`: Source node from which header information will be copied

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - Direct field access to node->count
- Called from (representative examples):
  - [RT_GROW_NODE_48](RT_GROW_NODE_48.md) (when growing a node48 to node256)
  - [RT_GROW_NODE_16](RT_GROW_NODE_16.md) (when growing a node16 to node48 or node256)
  - [RT_GROW_NODE_4](RT_GROW_NODE_4.md) (when growing a node4 to node16)
  - [RT_SHRINK_NODE_256](RT_SHRINK_NODE_256.md) (when shrinking a node256 to node48)
  - [RT_SHRINK_NODE_48](RT_SHRINK_NODE_48.md) (when shrinking a node48 to node16)
  - [RT_SHRINK_NODE_16](RT_SHRINK_NODE_16.md) (when shrinking a node16 to node4)

## Notes and Other Information
- This is an inline function for performance, as it's called frequently during node restructuring operations
- Currently only copies the count field, but the design allows for easy extension if more common fields are added to node headers
- Essential for maintaining data integrity during node growth and shrinking operations in the radix tree
- The function operates on RT_CHILD_PTR structures, which contain both allocation pointers and local pointers to the actual node data
- Part of the templated implementation, so the actual function name varies based on the RT_PREFIX used