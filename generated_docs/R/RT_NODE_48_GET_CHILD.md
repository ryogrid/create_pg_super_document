# RT_NODE_48_GET_CHILD

## Location
src/include/lib/radixtree.h: 784 - 790

## Overview
A macro that expands to a function name for retrieving a pointer to the child pointer slot associated with a specific chunk (key byte) in a node48 structure of the radix tree.

## Definition
```c
#define RT_NODE_48_GET_CHILD RT_MAKE_NAME(node_48_get_child)

static inline RT_PTR_ALLOC *
RT_NODE_48_GET_CHILD(RT_NODE_48 * node, uint8 chunk)
{
	return &node->children[node->slot_idxs[chunk]];
}
```

## Detailed Description
RT_NODE_48_GET_CHILD is a macro that generates a function name for retrieving a pointer to the child pointer slot for a given chunk in a node48 structure. This function is the counterpart to RT_NODE_48_IS_CHUNK_USED and provides access to the actual child pointer once it's known that a chunk exists.

The function implements the two-level indexing scheme used by node48:

1. It uses the chunk value to index into the slot_idxs array to get the slot position
2. It uses that slot position to index into the children array to get the pointer to the child pointer slot

The returned value is a pointer to the RT_PTR_ALLOC element in the children array, not the child pointer itself. This allows callers to both read and modify the child pointer as needed.

**Important**: This function should only be called after verifying that the chunk exists using RT_NODE_48_IS_CHUNK_USED, or after confirming that slot_idxs[chunk] is not RT_INVALID_SLOT_IDX. Calling this function with an invalid chunk will result in accessing an undefined index in the children array.

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE_48 structure containing the child
- `chunk`: The 8-bit chunk value (key byte) for which to retrieve the child pointer slot

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - RT_NODE_48 (structure type for 48-way radix tree nodes)
  - RT_PTR_ALLOC (type representing pointer allocation slots)
- Called from (representative examples):
  - RT_NODE_SEARCH (at src/include/lib/radixtree.h:1070)
  - RT_FREE_RECURSE (at src/include/lib/radixtree.h:2019)
  - RT_NODE_ITERATE_NEXT (at src/include/lib/radixtree.h:2178)

## Notes and Other Information
This function is a critical part of the node48 implementation in PostgreSQL's radix tree. It provides O(1) access to child pointers while maintaining the memory efficiency of the sparse representation.

The function is typically used in conjunction with RT_NODE_48_IS_CHUNK_USED:
1. First, check if the chunk exists using RT_NODE_48_IS_CHUNK_USED
2. If it exists, retrieve the child pointer slot using RT_NODE_48_GET_CHILD
3. Access or modify the child pointer through the returned slot pointer

The function is used in various tree operations:
- Tree traversal during search operations
- Child pointer access during iteration
- Memory cleanup when freeing tree nodes
- Tree verification and debugging

The returned pointer can be used to read the current child pointer value or to update it with a new child pointer, making this function essential for both read and write operations on the tree structure.