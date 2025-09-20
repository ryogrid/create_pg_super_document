# RT_NODE_SEARCH

## Location
[src/include/lib/radixtree.h:1042-1093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1042-L1093)

## Overview
RT_NODE_SEARCH is a macro that expands to a function that searches for a specific byte chunk within a radix tree node and returns a pointer to the corresponding child node slot.

## Definition

```c
static inline RT_PTR_ALLOC *
RT_NODE_SEARCH(RT_NODE * node, uint8 chunk)
```
## Detailed Description
RT_NODE_SEARCH is a macro-generated function that implements the core search functionality within radix tree nodes. It takes a node and a byte chunk (8-bit key fragment) and searches for that chunk within the node's structure. The function uses a switch statement to handle different node types (4, 16, 48, and 256) with type-specific optimized search algorithms.

For RT_NODE_KIND_4 nodes, it performs a linear search through the chunks array. For RT_NODE_KIND_16 nodes, it delegates to RT_NODE_16_SEARCH_EQ for optimized searching. For RT_NODE_KIND_48 nodes, it uses the slot_idxs array for direct indexing. For RT_NODE_KIND_256 nodes, it first checks if the chunk is used, then retrieves the child directly.

## Parameters / Member Variables
- : Pointer to the radix tree node to search within (must be a local pointer, not NULL)
- : 8-bit key fragment (byte) to search for within the node

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro expansion)
  - [RT_NODE_16_SEARCH_EQ](RT_NODE_16_SEARCH_EQ.md)
  - [RT_NODE_48_GET_CHILD](RT_NODE_48_GET_CHILD.md)
  - [RT_NODE_256_IS_CHUNK_USED](RT_NODE_256_IS_CHUNK_USED.md)
  - [RT_NODE_256_GET_CHILD](RT_NODE_256_GET_CHILD.md)
  - Assert
  - pg_unreachable
- Called from (representative examples):
  - [RT_FIND](RT_FIND.md)
  - [RT_GET_SLOT_RECURSIVE](RT_GET_SLOT_RECURSIVE.md)
  - [RT_DELETE_RECURSIVE](RT_DELETE_RECURSIVE.md)

## Notes and Other Information
- The function assumes the input node pointer has already been converted to a local pointer (asserted at runtime)
- Returns NULL if the chunk is not found in the node
- Returns a pointer to the child slot (RT_PTR_ALLOC *) if the chunk is found
- Uses different search strategies optimized for each node type's internal structure
- Part of the generic radixtree template system, where RT_MAKE_NAME generates type-specific function names