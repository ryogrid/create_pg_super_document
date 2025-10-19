# RT_SHRINK_NODE_256

## Location
[src/include/lib/radixtree.h:2336-2372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2336-L2372)

## Overview
A macro that defines a function name for shrinking a node256 to a node48 in the radix tree when the node becomes sufficiently sparse.

## Definition
```c
#define RT_SHRINK_NODE_256 RT_MAKE_NAME(shrink_child_256)
```

The actual function implementation:
```c
static void pg_noinline
RT_SHRINK_NODE_256(RT_RADIX_TREE * tree, RT_PTR_ALLOC * parent_slot, RT_CHILD_PTR node, uint8 chunk)
```

## Detailed Description
This macro creates a template function name for converting a node256 (which can hold up to 256 children) to a node48 (which can hold up to 48 children) when the node becomes sparse enough to warrant shrinking. The function allocates a new node48, copies all existing entries from the node256, and properly initializes the node48's slot index mapping and isset bitmap.

The function uses an efficient bitmap initialization technique, filling the isset array with a single store operation by creating a bitmask with the appropriate number of bits set. This assumes the node count is at most BITS_PER_BITMAPWORD.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that references this node 
- `node`: The node256 to be shrunk
- `chunk`: The chunk value (not used in this function but maintained for API consistency)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md) (allocates a new node)
  - [RT_COPY_COMMON](RT_COPY_COMMON.md) (copies common node metadata)
  - [RT_NODE_256_IS_CHUNK_USED](RT_NODE_256_IS_CHUNK_USED.md) (checks if a chunk is present in node256)
  - [RT_FREE_NODE](RT_FREE_NODE.md) (frees the old node)
- Called from (representative examples):
  - [RT_REMOVE_CHILD_256](RT_REMOVE_CHILD_256.md) (src/include/lib/radixtree.h:2396)

## Notes and Other Information
- This is part of PostgreSQL's template-based radix tree implementation
- The function is marked pg_noinline to prevent inlining for code size optimization
- Assumes deletion has already occurred in the caller before shrinking
- Uses efficient bitmap initialization: `((uint64) 1 << count) - 1` to set the first `count` bits
- Updates parent reference and frees the old node atomically
- Only triggers when node256 becomes sufficiently sparse to justify the conversion overhead

## Simplified Source

```c
static void pg_noinline
RT_SHRINK_NODE_256(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk)
{
    RT_NODE_256 *n256 = (RT_NODE_256 *) node.local;
    RT_CHILD_PTR newnode;
    RT_NODE_48 *new48;
    int slot_idx = 0;

    // Allocate new smaller node
    newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_48, RT_CLASS_48);
    new48 = (RT_NODE_48 *) newnode.local;

    // Copy common metadata and compress children array
    RT_COPY_COMMON(newnode, node);
    for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
    {
        if (RT_NODE_256_IS_CHUNK_USED(n256, i))
        {
            new48->slot_idxs[i] = slot_idx;         // Map chunk to slot index
            new48->children[slot_idx] = n256->children[i];  // Copy child pointer
            slot_idx++;
        }
    }

    // Set all bits for existing children in one operation
    new48->isset[0] = ((uint64) 1 << n256->base.count) - 1;

    // Replace old node with new one
    *parent_slot = newnode.alloc;
    RT_FREE_NODE(tree, node);
}
```