# RT_NODE_256_GET_CHILD

## Location
src/include/lib/radixtree.h: 800 - 809

## Overview
A macro that expands to a function returning a pointer to a child node in a 256-way radix tree node for a given chunk value.

## Definition
```c
#define RT_NODE_256_GET_CHILD RT_MAKE_NAME(node_256_get_child)

static inline RT_PTR_ALLOC *
RT_NODE_256_GET_CHILD(RT_NODE_256 * node, uint8 chunk)
{
    Assert(RT_NODE_256_IS_CHUNK_USED(node, chunk));
    return &node->children[chunk];
}
```

## Detailed Description
This function retrieves a pointer to a child node in a 256-way radix tree node using direct array indexing. The RT_NODE_256 structure maintains an array of 256 child pointers, one for each possible byte value (0-255). This function provides direct access to the child pointer at the specified chunk index.

The function includes an assertion that verifies the requested chunk actually has a child present by calling RT_NODE_256_IS_CHUNK_USED. This is a safety measure to ensure the caller doesn't attempt to access non-existent children.

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE_256 structure containing the child array
- `chunk`: An 8-bit value (0-255) representing the key byte used as an index into the children array

## Dependencies
- Functions called/Symbols referenced:
  - RT_NODE_256_IS_CHUNK_USED (for assertion checking)
  - RT_MAKE_NAME (macro name generation)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - RT_NODE_SEARCH (during tree traversal)
  - RT_ADD_CHILD_256 (when adding new children)
  - RT_FREE_RECURSE (during tree cleanup)
  - RT_NODE_ITERATE_NEXT (during iteration)

## Notes and Other Information
- Returns a pointer to RT_PTR_ALLOC, which is defined as dsa_pointer for shared memory allocation
- The direct indexing makes this function extremely efficient - O(1) lookup time
- This function assumes the caller has already verified the child exists using RT_NODE_256_IS_CHUNK_USED or similar checks
- Part of PostgreSQL's generic radix tree implementation for high-performance key-value storage
- The 256-way node is the largest and most direct node type in the radix tree hierarchy