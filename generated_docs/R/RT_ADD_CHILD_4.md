# RT_ADD_CHILD_4

## Location
[src/include/lib/radixtree.h:1513-1540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1513-L1540)

## Overview
A macro that generates the function name for adding a child to a radix tree node-4 structure, providing the most compact node type in PostgreSQL's adaptive radix tree implementation.

## Definition
```c
#define RT_ADD_CHILD_4 RT_MAKE_NAME(add_child_4)
```

## Detailed Description
RT_ADD_CHILD_4 is a macro that expands to a function name for adding a child node to a radix tree node of type 4. This represents the smallest and most memory-efficient node type in PostgreSQL's adaptive radix tree implementation, capable of holding up to 4 children. Node-4 is the starting point for most internal nodes in the radix tree, providing optimal memory usage for sparsely populated subtrees.

The node-4 implementation uses a simple linear search strategy since the small number of children makes this approach efficient. When a node-4 reaches its capacity and needs to accommodate another child, it triggers a growth operation to promote it to a node-16.

## Parameters / Member Variables
This macro expands to a function that typically takes:
- `tree`: Pointer to the radix tree structure
- `node`: Pointer to the node-4 where the child will be added
- `chunk`: The key chunk (byte value) that indexes the child being added

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
- Called from (representative examples):
  - [RT_NODE_INSERT](RT_NODE_INSERT.md) (when handling RT_NODE_KIND_4 case and node doesn't need to grow)
- Related symbols:
  - [RT_GROW_NODE_4](RT_GROW_NODE_4.md) (used when node-4 reaches capacity)
  - RT_NODE_MUST_GROW (condition check for growth)

## Notes and Other Information
- Part of PostgreSQL's adaptive radix tree implementation designed for memory efficiency
- [Node](../N/Node.md)-4 is the smallest internal node type (RT_NODE_KIND_4 = 0x00)
- Uses linear search for child lookup due to small fanout
- Most memory-efficient option for sparsely populated subtrees
- Automatically promotes to node-16 when capacity is exceeded
- Critical for the adaptive nature of the radix tree, starting small and growing as needed
- The actual function implementation handles the insertion logic specific to the 4-child node layout

## Simplified Source

```c
static inline RT_PTR_ALLOC *
RT_ADD_CHILD_4(RT_RADIX_TREE *tree, RT_CHILD_PTR node, uint8 chunk)
{
    RT_NODE_4 *n4 = (RT_NODE_4 *) node.local;
    int count = n4->base.count;

    // Find correct insertion position to maintain sorted order
    int insertpos = RT_NODE_4_GET_INSERTPOS(n4, chunk, count);

    // Shift existing chunks and children to make room
    RT_SHIFT_ARRAYS_FOR_INSERT(n4->chunks, n4->children, count, insertpos);

    // Insert new chunk at the correct position
    n4->chunks[insertpos] = chunk;

    // Update count and return child slot pointer
    n4->base.count++;
    return &n4->children[insertpos];
}
```