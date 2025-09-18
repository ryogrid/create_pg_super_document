# RT_ADD_CHILD_48

## Location
[src/include/lib/radixtree.h:1335-1372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1335-L1372)

## Overview
A macro that resolves to a static inline function for adding a child node to a node48 by finding the first available slot and updating the indirection array and bitmap.

## Definition
```c
#define RT_ADD_CHILD_48 RT_MAKE_NAME(add_child_48)

static inline RT_PTR_ALLOC *
RT_ADD_CHILD_48(RT_RADIX_TREE * tree, RT_CHILD_PTR node, uint8 chunk)
```

## Detailed Description
This function adds a child to a node48 by finding the first available slot in the child array and establishing the mapping from chunk value to slot index. Node48 uses an indirection mechanism where `slot_idxs[chunk]` maps each possible chunk value (0-255) to one of 48 actual child slots. The function scans the bitmap `isset` to find the first unset bit, which indicates the first available slot, then sets up the mapping and marks the slot as used. The bitmap manipulation uses efficient bit operations to find the rightmost zero bit.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `node`: The node48 to add a child to
- `chunk`: The 8-bit key fragment that will map to the new child slot

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - RT_BM_IDX (calculates bitmap array index)
  - bmw_rightmost_one_pos (finds position of rightmost set bit)
  - [RT_VERIFY_NODE](RT_VERIFY_NODE.md) (node verification macro)
- Called from (representative examples):
  - [RT_NODE_INSERT](RT_NODE_INSERT.md) (at src/include/lib/radixtree.h:1567)

## Notes and Other Information
Node48 provides a balance between node16 (which has limited capacity) and node256 (which may waste space). It uses an indirection array to map 256 possible chunk values to 48 actual slots, allowing efficient storage when the key space is sparse. The bitmap `isset` tracks which of the 48 slots are occupied, and the algorithm efficiently finds the first available slot using bit manipulation. The function updates both the slot mapping (`slot_idxs[chunk] = insertpos`) and marks the slot as used in the bitmap.