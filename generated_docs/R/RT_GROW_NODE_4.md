# RT_GROW_NODE_4

## Location
[src/include/lib/radixtree.h:1479-1512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1479-L1512)

## Overview
A macro that generates the function name for growing a radix tree node-4 to a larger node type when it reaches capacity, enabling dynamic node size expansion in radix trees.

## Definition
```c
#define RT_GROW_NODE_4 RT_MAKE_NAME(grow_node_4)
```

## Detailed Description
RT_GROW_NODE_4 is a macro that expands to a function name responsible for promoting a radix tree node-4 to a larger node type (typically node-16) when the node-4 reaches its maximum capacity of 4 children. This is a critical operation in the adaptive radix tree implementation, allowing the tree to dynamically adjust its internal structure based on the number of children at each node.

The growth operation involves allocating a new larger node, copying existing children from the node-4 to the new node, updating parent pointers, and then adding the new child that triggered the growth. This ensures optimal space utilization while maintaining fast access times.

## Parameters / Member Variables
This macro expands to a function that typically takes:
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent node's slot that references this node
- `node`: Pointer to the node-4 that needs to grow
- `chunk`: The key chunk (byte value) for the new child being added

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
- Called from (representative examples):
  - [RT_NODE_INSERT](RT_NODE_INSERT.md) (when RT_NODE_MUST_GROW condition is true for node-4)
- Related symbols:
  - RT_NODE_MUST_GROW (condition check)
  - [RT_ADD_CHILD_16](RT_ADD_CHILD_16.md) (typically used after growth)

## Notes and Other Information
- Part of PostgreSQL's adaptive radix tree implementation
- Triggered when a node-4's count equals its fanout (4 children)
- Usually promotes node-4 to node-16 for better capacity
- The growth operation is atomic from the tree's perspective
- After growth, the original insertion operation continues with the new larger node
- Critical for maintaining tree balance and preventing unnecessary depth increases
- The actual function implementation handles memory management and pointer updates during the node type transition

## Simplified Source

```c
static pg_noinline RT_PTR_ALLOC *
RT_GROW_NODE_4(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot,
               RT_CHILD_PTR node, uint8 chunk)
{
    RT_NODE_4 *n4 = (RT_NODE_4 *) node.local;
    RT_CHILD_PTR newnode;
    RT_NODE_16 *new16;

    // Allocate new node-16
    newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_16, RT_CLASS_16_LO);
    new16 = (RT_NODE_16 *) newnode.local;

    // Copy common fields and existing children with gap for new chunk
    RT_COPY_COMMON(newnode, node);
    int insertpos = RT_NODE_4_GET_INSERTPOS(n4, chunk, RT_FANOUT_4);
    RT_COPY_ARRAYS_FOR_INSERT(new16->chunks, new16->children,
                              n4->chunks, n4->children,
                              RT_FANOUT_4, insertpos);

    // Insert new chunk and update count
    new16->chunks[insertpos] = chunk;
    new16->base.count++;

    // Replace old node with new one
    *parent_slot = newnode.alloc;
    RT_FREE_NODE(tree, node);

    return &new16->children[insertpos];
}
```