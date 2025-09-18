# RT_NODE_256

## Location
[src/include/lib/radixtree.h:568-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L568-L577)

## Overview
RT_NODE_256 is a macro that generates a type name for a 256-slot adaptive radix tree node structure, representing the largest node type in PostgreSQL's templated radix tree implementation.

## Definition


## Detailed Description
RT_NODE_256 is part of PostgreSQL's adaptive radix tree (ART) implementation and represents the largest node variant in the four-tier node hierarchy (4, 16, 48, and 256 slots). This macro generates a prefixed type name for the 256-slot node structure, which is used when a node needs to store a large number of child pointers.

The actual structure definition for the 256-slot node contains:
- A base RT_NODE header with common node metadata
- A bitmap (isset array) to track which of the 256 slots are currently in use
- A direct array of 256 child pointers, providing O(1) access time

This node type is used when a node needs to store more than 48 child pointers, representing the final growth stage in the adaptive radix tree hierarchy. Unlike smaller node types that use indirection or compressed representations, the 256-slot node provides direct indexing where each possible byte value (0-255) maps directly to an array index.

## Parameters / Member Variables
- : RT_NODE structure containing common node metadata (kind, count, fanout)
- : Bitmap array tracking which of the 256 slots in the children array are occupied
- : Array of 256 child pointers, directly indexed by byte values (0-255)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX
  - RT_PREFIX
- Called from (representative examples):
  - [RT_NODE_256_IS_CHUNK_USED](RT_NODE_256_IS_CHUNK_USED.md)
  - [RT_NODE_256_GET_CHILD](RT_NODE_256_GET_CHILD.md)
  - [RT_ALLOC_NODE](RT_ALLOC_NODE.md)
  - [RT_NODE_SEARCH](RT_NODE_SEARCH.md)
  - [RT_ADD_CHILD_256](RT_ADD_CHILD_256.md)
  - [RT_GROW_NODE_48](RT_GROW_NODE_48.md)
  - [RT_FREE_RECURSE](RT_FREE_RECURSE.md)
  - [RT_NODE_ITERATE_NEXT](RT_NODE_ITERATE_NEXT.md)
  - [RT_SHRINK_NODE_256](RT_SHRINK_NODE_256.md)
  - [RT_REMOVE_CHILD_256](RT_REMOVE_CHILD_256.md)
  - [RT_VERIFY_NODE](RT_VERIFY_NODE.md)

## Notes and Other Information
The 256-slot node represents the most space-consuming but also most performance-optimized variant in the adaptive radix tree design:

1. **Direct Indexing**: Unlike smaller node types, it provides direct O(1) access using byte values as array indices, eliminating the need for search or indirection
2. **Maximum Capacity**: Can store up to 256 child pointers, accommodating the full range of possible byte values
3. **Memory Trade-off**: Uses more memory than smaller node types but provides the fastest access times
4. **Automatic Shrinking**: When the number of children drops significantly (typically below 48), the node automatically shrinks to a smaller, more memory-efficient RT_NODE_48

The 256-slot node is particularly effective for dense key distributions where many different byte values are present at a given tree level. It represents the final stage of the adaptive growth strategy, balancing maximum performance with reasonable memory usage for high-density scenarios.