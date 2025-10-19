# RT_ADD_CHILD_16

## Location
[src/include/lib/radixtree.h:1460-1478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1460-L1478)

## Overview
A macro that generates the function name for adding a child to a radix tree node-16 structure, creating a type-safe interface for radix tree node manipulation.

## Definition
```c
#define RT_ADD_CHILD_16 RT_MAKE_NAME(add_child_16)
```

## Detailed Description
RT_ADD_CHILD_16 is a macro that expands to a function name for adding a child node to a radix tree node of type 16. This is part of PostgreSQL's generic radix tree implementation where different node types (4, 16, 48, 256) have different capacities and internal structures. The macro uses the RT_MAKE_NAME infrastructure to generate type-specific function names based on the current radix tree prefix configuration, allowing the same codebase to work with different radix tree instantiations.

Node-16 represents a radix tree node that can hold up to 16 children, typically used when a node-4 grows beyond its capacity but hasn't yet reached the threshold for promotion to node-48.

## Parameters / Member Variables
This macro expands to a function that typically takes:
- `tree`: Pointer to the radix tree structure
- `node`: Pointer to the node-16 where the child will be added  
- `chunk`: The key chunk (byte value) that indexes the child being added

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
- Called from (representative examples):
  - [RT_NODE_INSERT](RT_NODE_INSERT.md) (when handling RT_NODE_KIND_16 case)

## Notes and Other Information
- Part of the generic radix tree implementation in PostgreSQL
- Works in conjunction with RT_GROW_NODE_4 when a node-4 needs to be promoted to node-16
- The actual function implementation is generated through the macro expansion system
- [Node](../N/Node.md)-16 uses a more compact representation than node-48 or node-256 for space efficiency
- This macro is used in the main insertion logic within RT_NODE_INSERT to handle the specific case of adding children to 16-capacity nodes

## Simplified Source

```c
static inline RT_PTR_ALLOC *
RT_ADD_CHILD_16(RT_RADIX_TREE *tree, RT_CHILD_PTR node, uint8 chunk)
{
    RT_NODE_16 *n16 = (RT_NODE_16 *) node.local;

    // Find correct insertion position to maintain sorted order
    int insertpos = RT_NODE_16_GET_INSERTPOS(n16, chunk);

    // Shift existing chunks and children to make room
    RT_SHIFT_ARRAYS_FOR_INSERT(n16->chunks, n16->children,
                               n16->base.count, insertpos);

    // Insert new chunk at the correct position
    n16->chunks[insertpos] = chunk;

    // Update count and return child slot pointer
    n16->base.count++;
    return &n16->children[insertpos];
}
```