# RT_GROW_NODE_48

## Location
[src/include/lib/radixtree.h:1288-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1288-L1334)

## Overview
A macro that resolves to a static pg_noinline function for growing a node48 to a node256 when the node48 becomes full and needs to accommodate a new child.

## Definition
```c
#define RT_GROW_NODE_48 RT_MAKE_NAME(grow_node_48)

static pg_noinline RT_PTR_ALLOC *
RT_GROW_NODE_48(RT_RADIX_TREE * tree, RT_PTR_ALLOC * parent_slot, RT_CHILD_PTR node,
                uint8 chunk)
```

## Detailed Description
This function transforms a full node48 into a node256 to accommodate additional children. Node48 uses an indirection array (`slot_idxs`) to map 256 possible chunk values to 48 actual child slots, while node256 provides direct indexing with a bitmap to track occupied slots. The function efficiently converts the indirection-based storage to direct storage by iterating through all 256 possible chunk values, checking if they exist in the node48, and setting the corresponding bits in the node256 bitmap. The conversion is optimized by processing bits word-at-a-time rather than individually.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's reference to this node (updated to point to new node)
- `node`: The full node48 to be grown
- `chunk`: The new key fragment that triggered the growth

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md) (allocates new node256)
  - [RT_COPY_COMMON](RT_COPY_COMMON.md) (copies common node fields)
  - RT_BM_IDX (bitmap index calculation)
  - [RT_FREE_NODE](RT_FREE_NODE.md) (deallocates old node48)
  - [RT_ADD_CHILD_256](RT_ADD_CHILD_256.md) (adds the new child to the grown node)
- Called from (representative examples):
  - [RT_NODE_INSERT](RT_NODE_INSERT.md) (at src/include/lib/radixtree.h:1565)

## Notes and Other Information
The function is marked as pg_noinline because node growth is a relatively rare operation that should not be inlined to avoid code bloat. The conversion algorithm processes chunks in bitmap word-sized batches for efficiency, building the bitmap word by word. After conversion, the old node48 is freed and the parent reference is updated to point to the new node256. The function concludes by adding the new child that triggered the growth operation.

## Simplified Source

```c
static pg_noinline RT_PTR_ALLOC *
RT_GROW_NODE_48(RT_RADIX_TREE *tree, RT_PTR_ALLOC *parent_slot, RT_CHILD_PTR node, uint8 chunk)
{
    RT_NODE_48 *n48 = (RT_NODE_48 *) node.local;
    RT_CHILD_PTR newnode;
    RT_NODE_256 *new256;
    int i = 0;

    // Allocate new node256
    newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_256, RT_CLASS_256);
    new256 = (RT_NODE_256 *) newnode.local;

    // Copy common node fields
    RT_COPY_COMMON(newnode, node);

    // Convert from indirection-based (node48) to direct (node256) storage
    for (int word_num = 0; word_num < RT_BM_IDX(RT_NODE_MAX_SLOTS); word_num++) {
        bitmapword bitmap = 0;

        // Process chunks in bitmap word-sized batches for efficiency
        for (int bit = 0; bit < BITS_PER_BITMAPWORD; bit++) {
            uint8 offset = n48->slot_idxs[i];

            if (offset != RT_INVALID_SLOT_IDX) {
                // Set bit in bitmap and copy child pointer
                bitmap |= ((bitmapword) 1 << bit);
                new256->children[i] = n48->children[offset];
            }
            i++;
        }
        new256->isset[word_num] = bitmap;
    }

    // Replace old node with new one
    *parent_slot = newnode.alloc;
    RT_FREE_NODE(tree, node);

    // Add the new child that triggered this growth
    return RT_ADD_CHILD_256(tree, newnode, chunk);
}
```