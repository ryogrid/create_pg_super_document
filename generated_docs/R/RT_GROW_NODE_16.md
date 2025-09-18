# RT_GROW_NODE_16

## Location
src/include/lib/radixtree.h: 1373 - 1459

## Overview
A macro that resolves to a static pg_noinline function for growing a node16 when it becomes full, either expanding to a larger node16 or transitioning to a node48.

## Definition
```c
#define RT_GROW_NODE_16 RT_MAKE_NAME(grow_node_16)

static pg_noinline RT_PTR_ALLOC *
RT_GROW_NODE_16(RT_RADIX_TREE * tree, RT_PTR_ALLOC * parent_slot, RT_CHILD_PTR node,
                uint8 chunk)
```

## Detailed Description
This function handles the growth of a full node16 through two possible transitions: 1) If the node16 is at low capacity (RT_FANOUT_16_LO), it grows to high capacity (RT_FANOUT_16_HI) within the same node type, copying arrays with space for insertion; 2) If already at high capacity, it transitions to a node48, converting from the sorted array storage of node16 to the indirection-based storage of node48. The function uses RT_COPY_ARRAYS_FOR_INSERT for the first case and sets up the slot mapping and bitmap for the second case. In both cases, the old node is freed and parent references are updated.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's reference to this node (updated to point to new node)
- `node`: The full node16 to be grown
- `chunk`: The new key fragment that triggered the growth

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md) (allocates new node)
  - [RT_COPY_COMMON](RT_COPY_COMMON.md) (copies common node fields)
  - [RT_NODE_16_GET_INSERTPOS](RT_NODE_16_GET_INSERTPOS.md) (finds insertion position in sorted array)
  - [RT_COPY_ARRAYS_FOR_INSERT](RT_COPY_ARRAYS_FOR_INSERT.md) (copies arrays with gap for insertion)
  - [RT_VERIFY_NODE](RT_VERIFY_NODE.md) (node verification macro)
  - [RT_FREE_NODE](RT_FREE_NODE.md) (deallocates old node)
  - RT_BM_IDX/RT_BM_BIT (bitmap index/bit calculations)
- Called from (representative examples):
  - [RT_NODE_INSERT](RT_NODE_INSERT.md) (at src/include/lib/radixtree.h:1558)

## Notes and Other Information
The function implements two distinct growth strategies based on the current fanout. For low-to-high capacity growth within node16, it maintains sorted order and uses array copying. For node16-to-node48 transition, it converts from sorted arrays to indirection mapping, setting up the `slot_idxs` array to map chunk values to child positions and initializing the `isset` bitmap efficiently with a single store operation since RT_FANOUT_16_HI fits within a bitmapword. The function is marked pg_noinline as node growth is relatively infrequent.