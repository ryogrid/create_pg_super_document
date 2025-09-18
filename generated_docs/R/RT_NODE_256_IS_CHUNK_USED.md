# RT_NODE_256_IS_CHUNK_USED

## Location
[src/include/lib/radixtree.h:791-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L791-L799)

## Overview
A macro that expands to a function checking whether a specific chunk (byte value) has a corresponding child node in a 256-way radix tree node.

## Definition
```c
#define RT_NODE_256_IS_CHUNK_USED RT_MAKE_NAME(node_256_is_chunk_used)

static inline bool
RT_NODE_256_IS_CHUNK_USED(RT_NODE_256 * node, uint8 chunk)
{
    int idx = RT_BM_IDX(chunk);
    int bitnum = RT_BM_BIT(chunk);
    
    return (node->isset[idx] & ((bitmapword) 1 << bitnum)) != 0;
}
```

## Detailed Description
This function determines if a given chunk value (0-255) corresponds to an active child pointer in a 256-way radix tree node. The RT_NODE_256 type uses a bitmap array (`isset`) to efficiently track which of the 256 possible child slots are currently in use. The function performs a bitmap lookup by calculating the appropriate word index and bit position within that word, then checks if the corresponding bit is set.

The implementation uses two helper macros:
- RT_BM_IDX(x) calculates which bitmapword contains the bit for chunk x
- RT_BM_BIT(x) calculates the bit position within that bitmapword

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE_256 structure containing the bitmap and child array
- `chunk`: An 8-bit value (0-255) representing the key byte being looked up

## Dependencies
- Functions called/Symbols referenced:
  - RT_BM_IDX (bitmap word index calculation)
  - RT_BM_BIT (bitmap bit position calculation)
  - RT_MAKE_NAME (macro name generation)
- Called from (representative examples):
  - [RT_NODE_256_GET_CHILD](RT_NODE_256_GET_CHILD.md) (for assertion checking)
  - [RT_NODE_SEARCH](RT_NODE_SEARCH.md) (during tree traversal)
  - [RT_FREE_RECURSE](RT_FREE_RECURSE.md) (during tree cleanup)
  - [RT_NODE_ITERATE_NEXT](RT_NODE_ITERATE_NEXT.md) (during iteration)
  - [RT_SHRINK_NODE_256](RT_SHRINK_NODE_256.md) (during node optimization)

## Notes and Other Information
- This is part of PostgreSQL's generic radix tree implementation for efficient key-value storage
- The 256-way node is the largest node type in the radix tree, providing direct indexing for all possible byte values
- The bitmap approach allows efficient memory usage - only checking a few bits rather than scanning the entire 256-element array
- This function is typically used as a guard before accessing child nodes to ensure they exist