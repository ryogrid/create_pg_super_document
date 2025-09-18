# RT_GROW_NODE_48

## Location
src/include/lib/radixtree.h: 1288 - 1334

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
  - RT_ALLOC_NODE (allocates new node256)
  - RT_COPY_COMMON (copies common node fields)
  - RT_BM_IDX (bitmap index calculation)
  - RT_FREE_NODE (deallocates old node48)
  - RT_ADD_CHILD_256 (adds the new child to the grown node)
- Called from (representative examples):
  - RT_NODE_INSERT (at src/include/lib/radixtree.h:1565)

## Notes and Other Information
The function is marked as pg_noinline because node growth is a relatively rare operation that should not be inlined to avoid code bloat. The conversion algorithm processes chunks in bitmap word-sized batches for efficiency, building the bitmap word by word. After conversion, the old node48 is freed and the parent reference is updated to point to the new node256. The function concludes by adding the new child that triggered the growth operation.