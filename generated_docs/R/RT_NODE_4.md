# RT_NODE_4

## Location
[src/include/lib/radixtree.h:518-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L518-L526)

## Overview
RT_NODE_4 is a macro that expands to generate the smallest node type in the adaptive radix tree implementation, designed for nodes with up to 8 children.

## Definition
```c
#define RT_NODE_4 RT_MAKE_NAME(node_4)
```

Expands to the node structure:
```c
typedef struct RT_NODE_4
{
    RT_NODE base;
    uint8 chunks[RT_FANOUT_4_MAX];
    RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_4;
```

## Detailed Description
RT_NODE_4 represents the most compact internal node type in PostgreSQL's adaptive radix tree (ART) implementation. It is optimized for nodes with a small number of children (typically 1-8), using parallel arrays to store key chunks and corresponding child pointers. The actual expansion follows the pattern: `{prefix}_node_4`, where prefix is determined by RT_PREFIX.

This node type uses a simple linear search approach for child lookup, which is efficient for small fanouts. When the number of children exceeds the capacity, the node automatically grows to RT_NODE_16. The parallel array structure keeps key chunks and child pointers aligned at corresponding indices, with both arrays maintained in sorted order by chunk value.

## Parameters / Member Variables
- `base`: The RT_NODE base structure containing kind, fanout, and count metadata
- `chunks`: Array of key chunks (byte values) stored in sorted order, with size RT_FANOUT_4_MAX
- `children`: Flexible array of child pointers corresponding to the chunks at the same indices

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - [RT_NODE](RT_NODE.md) (base node structure)
  - RT_FANOUT_4_MAX (maximum capacity constant)
  - RT_PTR_ALLOC (child pointer type)
  - FLEXIBLE_ARRAY_MEMBER (variable-length array support)
- Called from (representative examples):
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md) (node allocation with RT_NODE_KIND_4)
  - [RT_NODE_SEARCH](RT_NODE_SEARCH.md) (child lookup using linear search)
  - [RT_NODE_4_GET_INSERTPOS](RT_NODE_4_GET_INSERTPOS.md) (finding insertion position)
  - [RT_ADD_CHILD_4](RT_ADD_CHILD_4.md) (adding new child entries)
  - [RT_GROW_NODE_4](RT_GROW_NODE_4.md) (converting to RT_NODE_16 when capacity exceeded)
  - [RT_REMOVE_CHILD_4](RT_REMOVE_CHILD_4.md) (removing child entries)

## Notes and Other Information
- Uses linear search for child lookup, optimal for small fanouts (1-8 children)
- Maintains parallel arrays with chunks and children at corresponding indices
- Both arrays are kept sorted by chunk value for consistent ordering
- Automatically grows to RT_NODE_16 when capacity is exceeded
- Can shrink from RT_NODE_16 when child count drops below threshold
- Most memory-efficient node type for sparse key distributions
- RT_FANOUT_4_MAX is calculated as (8 - sizeof(RT_NODE)) to maximize space utilization
- Part of the adaptive behavior that optimizes memory usage based on actual fanout requirements