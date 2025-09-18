# RT_SHRINK_NODE_48

## Location
src/include/lib/radixtree.h: 2404 - 2439

## Overview
A macro that defines a function name for shrinking a node48 to a node16 in the radix tree when the node becomes sufficiently sparse.

## Definition
```c
#define RT_SHRINK_NODE_48 RT_MAKE_NAME(shrink_child_48)
```

The actual function implementation:
```c
static void pg_noinline
RT_SHRINK_NODE_48(RT_RADIX_TREE * tree, RT_PTR_ALLOC * parent_slot, RT_CHILD_PTR node, uint8 chunk)
```

## Detailed Description
This macro creates a template function name for converting a node48 (which can hold up to 48 children) to a node16 (which can hold up to 16 children) when the node becomes sparse enough to warrant shrinking. The function allocates a new node16, iterates through all possible slots in the node48, and copies existing entries to the new node16 in a compact manner.

The function uses the RT_CLASS_16_LO size class for simplicity, skipping the larger node16 size class optimization. It scans all 256 possible chunk values and copies only those that have valid slot indices in the node48.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure
- `parent_slot`: Pointer to the parent's slot that references this node
- `node`: The node48 to be shrunk
- `chunk`: The chunk value (not used in this function but maintained for API consistency)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - RT_ALLOC_NODE (allocates a new node)
  - RT_COPY_COMMON (copies common node metadata)
  - RT_VERIFY_NODE (verifies node integrity)
  - RT_FREE_NODE (frees the old node)
  - RT_INVALID_SLOT_IDX (constant indicating invalid slot)
- Called from (representative examples):
  - RT_REMOVE_CHILD_48 (src/include/lib/radixtree.h:2464)

## Notes and Other Information
- This is part of PostgreSQL's template-based radix tree implementation
- The function is marked pg_noinline to prevent inlining for code size optimization
- Assumes deletion has already occurred in the caller before shrinking
- Scans all 256 possible chunk values to find valid entries in the sparse node48
- Uses RT_CLASS_16_LO size class for simplicity, avoiding larger node16 optimizations
- Maintains the destidx counter to track the number of copied entries
- Includes assertion to verify the destination index is within the new node's fanout limit
- Updates parent reference and frees the old node atomically