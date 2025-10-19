# RT_SHRINK_NODE_16

## Location
[src/include/lib/radixtree.h:2473-2497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2473-L2497)

## Overview
RT_SHRINK_NODE_16 is a macro that expands to a function responsible for converting a node16 to a smaller node4 while simultaneously deleting a specific entry in PostgreSQL's radix tree implementation.

## Definition
```c
#define RT_SHRINK_NODE_16 RT_MAKE_NAME(shrink_child_16)

static void pg_noinline
RT_SHRINK_NODE_16(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 deletepos)
```

## Detailed Description
This function performs a combined operation of node shrinking and entry deletion to optimize memory usage and maintain performance in the radix tree. It converts a node16 (which can hold up to 16 children) to a node4 (which can hold up to 4 children) while simultaneously removing the entry at the specified position. The function performs the following steps:

1. Allocates a new node4 structure
2. Initializes the new node with proper type information
3. Copies all existing entries from the node16 except the one at deletepos
4. Updates the count in the new node
5. Verifies the integrity of the new node
6. Updates the parent's reference to point to the new node
7. Frees the original node16

This combined approach is more efficient than separate delete and shrink operations because it avoids unnecessary copying operations.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that contains this node (updated to point to new node)
- `node`: The node16 to be shrunk
- `deletepos`: The position of the entry to be deleted during the shrinking process

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md)
  - [RT_COPY_COMMON](RT_COPY_COMMON.md)
  - [RT_COPY_ARRAYS_AND_DELETE](RT_COPY_ARRAYS_AND_DELETE.md)
  - [RT_VERIFY_NODE](RT_VERIFY_NODE.md)
  - [RT_FREE_NODE](RT_FREE_NODE.md)
- Called from (representative examples):
  - [RT_REMOVE_CHILD_16](RT_REMOVE_CHILD_16.md)

## Notes and Other Information
- This function is marked with pg_noinline attribute to prevent inlining for better code size optimization
- The shrinking threshold is hard-coded to 4, meaning when a node16 has 4 or fewer entries, it gets converted to node4
- Node4 uses linear search which is faster than SIMD for small counts (≤3 elements) on x86-64
- The function combines deletion and shrinking in one operation for efficiency
- Located in src/include/lib/radixtree.h at lines 2472-2495
- Part of PostgreSQL's generic radix tree implementation that uses C macros for type polymorphism

## Simplified Source

```c
static void pg_noinline
RT_SHRINK_NODE_16(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 deletepos)
{
    RT_NODE_16 *n16 = (RT_NODE_16 *) node.local;
    RT_CHILD_PTR newnode;
    RT_NODE_4 *new4;

    // Allocate new smaller node
    newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_4, RT_CLASS_4);
    new4 = (RT_NODE_4 *) newnode.local;

    // Copy entries excluding the one at deletepos (combined delete+shrink operation)
    RT_COPY_COMMON(newnode, node);
    RT_COPY_ARRAYS_AND_DELETE(new4->chunks, new4->children,
                              n16->chunks, n16->children,
                              n16->base.count, deletepos);

    new4->base.count--;  // Account for the deleted entry
    RT_VERIFY_NODE((RT_NODE *) new4);

    // Replace old node with new one
    *parent_slot = newnode.alloc;
    RT_FREE_NODE(tree, node);
}
```