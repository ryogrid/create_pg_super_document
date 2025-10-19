# RT_REMOVE_CHILD_4

## Location
[src/include/lib/radixtree.h:2523-2575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2523-L2575)

## Overview
RT_REMOVE_CHILD_4 is a macro that expands to a function responsible for removing a child entry from a node4, the smallest internal node type in PostgreSQL's radix tree implementation.

## Definition
```c
#define RT_REMOVE_CHILD_4 RT_MAKE_NAME(remove_child_4)

static inline void
RT_REMOVE_CHILD_4(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk, RT_PTR_ALLOC *slot)
```

## Detailed Description
This function removes a child entry from a node4 structure in the radix tree. As the smallest internal node type, node4 handles special cases that don't occur in larger nodes. The function handles two distinct scenarios:

1. **Last Entry Removal**: When removing the final entry from a node4:
   - If it's the root child node, it doesn't free the node but marks both the tree and root child node as empty, resetting tree metadata (start_shift and max_val)
   - If it's a non-root node, it frees the entire node and sets the parent slot to RT_INVALID_PTR_ALLOC to signal the parent level to delete its child pointer

2. **Regular Entry Removal**: For nodes with multiple entries, it performs in-place deletion by shifting the chunks and children arrays to remove the specified entry, then decrements the count

The special handling for the root child node ensures that RT_SET can always assume the root structure exists, simplifying tree operations.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that contains this node (for potential node deletion signaling)
- `node`: The node4 from which to remove the child
- `chunk`: The 8-bit chunk value identifying which child to remove
- `slot`: Pointer to the exact slot in the children array to be removed (used to calculate deletepos)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - [RT_SHIFT_GET_MAX_VAL](RT_SHIFT_GET_MAX_VAL.md)
  - [RT_FREE_NODE](RT_FREE_NODE.md)
  - [RT_SHIFT_ARRAYS_AND_DELETE](RT_SHIFT_ARRAYS_AND_DELETE.md)
  - RT_INVALID_PTR_ALLOC
- Called from (representative examples):
  - [RT_NODE_DELETE](RT_NODE_DELETE.md)

## Notes and Other Information
- This function is marked as static inline for performance optimization
- Node4 is the smallest internal node and cannot be shrunk further, so it handles terminal deletion cases
- Special root child node handling maintains tree structure invariants for efficient operations
- Uses RT_INVALID_PTR_ALLOC to signal upward deletion propagation when freeing entire nodes
- The function demonstrates the bottom-up deletion strategy used in radix trees
- Part of PostgreSQL's generic radix tree implementation that uses C macros for type polymorphism
- Located in src/include/lib/radixtree.h at lines 2523-2570

## Simplified Source

```c
static inline void
RT_REMOVE_CHILD_4(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk, RT_PTR_ALLOC *slot)
{
    RT_NODE_4 *n4 = (RT_NODE_4 *) node.local;

    if (n4->base.count == 1)
    {
        // Last entry removal: handle root vs non-root differently
        if (parent_slot == &tree->ctl->root)
        {
            // Root child node: keep structure but mark empty
            n4->base.count = 0;
            tree->ctl->start_shift = 0;
            tree->ctl->max_val = RT_SHIFT_GET_MAX_VAL(0);
        }
        else
        {
            // Non-root: free entire node and signal parent for deletion
            RT_FREE_NODE(tree, node);
            *parent_slot = RT_INVALID_PTR_ALLOC;  // Signal parent to delete child pointer
        }
    }
    else
    {
        // Regular entry removal: shift arrays to remove entry
        int deletepos = slot - n4->children;
        RT_SHIFT_ARRAYS_AND_DELETE(n4->chunks, n4->children, n4->base.count, deletepos);
        n4->base.count--;
    }
}
```