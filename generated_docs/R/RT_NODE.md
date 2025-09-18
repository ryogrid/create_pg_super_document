# RT_NODE

## Location
src/include/lib/radixtree.h: 374 - 395

## Overview
RT_NODE is a macro that expands to generate the base node structure type name for the radix tree implementation using the RT_MAKE_NAME macro expansion system.

## Definition
```c
#define RT_NODE RT_MAKE_NAME(node)
```

Expands to the base node structure:
```c
typedef struct RT_NODE
{
    uint8 kind;
    uint8 fanout; 
    uint8 count;
} RT_NODE;
```

## Detailed Description
RT_NODE defines the base structure for all internal nodes in PostgreSQL's radix tree implementation. It serves as the common header for all node types (RT_NODE_4, RT_NODE_16, RT_NODE_48, RT_NODE_256) in the adaptive radix tree structure. The actual expansion follows the pattern: `{prefix}_node`, where prefix is determined by RT_PREFIX.

This base node structure contains essential metadata that enables the radix tree to dynamically adapt its internal structure based on the number of children at each node. The adaptive nature allows the tree to optimize memory usage and performance by using different node layouts depending on the fanout requirements.

## Parameters / Member Variables
- `kind`: Node type identifier indicating which specific node variant this is (NODE_4, NODE_16, NODE_48, or NODE_256)
- `fanout`: Maximum capacity for the current size class, enabling multiple size classes per node kind. Overflows to zero for NODE_256 since it never needs to grow
- `count`: Current number of children in this node. For NODE_256, zero is interpreted as 256 since it cannot have zero children

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - RT_NODE_4, RT_NODE_16, RT_NODE_48, RT_NODE_256 (specific node type implementations)
- Called from (representative examples):
  - RT_ALLOC_NODE (node allocation functions)
  - RT_NODE_SEARCH (node traversal functions)
  - RT_NODE_INSERT (node insertion functions)
  - RT_GROW_NODE_* (node expansion functions)
  - RT_SHRINK_NODE_* (node contraction functions)
  - RT_VERIFY_NODE (node validation functions)

## Notes and Other Information
- Serves as the base structure embedded in all specialized node types
- Uses uint8 for all fields to minimize memory overhead
- The adaptive design allows nodes to grow and shrink dynamically based on usage
- NODE_256 has special handling where count=0 represents 256 children
- The kind field enables runtime polymorphism for different node operations
- All node types include this base structure as their first member for consistent access patterns
- Part of the Adaptive Radix Tree (ART) algorithm implementation in PostgreSQL