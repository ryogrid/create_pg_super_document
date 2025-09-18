# RT_FIND

## Location
src/include/lib/radixtree.h: 1094 - 1133

## Overview
RT_FIND is a macro that expands to a function that searches for a key in the radix tree and returns a pointer to the associated value if found.

## Definition
```c
#define RT_FIND RT_MAKE_NAME(find)

RT_VALUE_TYPE *
RT_FIND(RT_RADIX_TREE *tree, uint64 key)
```

## Detailed Description
RT_FIND is the primary lookup function for the radix tree data structure. It implements a top-down traversal through the tree nodes, following the path defined by the key's bit chunks. The function starts from the root node and descends through the tree using RT_NODE_SEARCH to find the appropriate child for each key chunk until it reaches a leaf or encounters a missing path.

The function first validates that the key doesn't exceed the tree's maximum value, then iteratively processes key chunks using the tree's configured shift values. At each level, it extracts the relevant key chunk using RT_GET_KEY_CHUNK and searches for it in the current node. If any chunk is not found during traversal, the function returns NULL indicating the key doesn't exist.

When the traversal reaches the bottom level (shift < 0), the function checks whether the final slot contains a direct value pointer or a node pointer, and returns the appropriate value.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure to search in
- `key`: 64-bit unsigned integer key to look up in the tree

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro expansion)
  - RT_NODE_SEARCH
  - RT_GET_KEY_CHUNK
  - RT_PTR_SET_LOCAL
  - RT_CHILDPTR_IS_VALUE
  - RT_PTR_ALLOC_IS_VALID
  - Assert
- Called from (representative examples):
  - RT_HANDLE (macro-generated handle function)

## Notes and Other Information
- Returns NULL if the key is not found or exceeds tree->ctl->max_val
- Returns a pointer to RT_VALUE_TYPE if the key is found
- In shared memory builds (RT_SHMEM), validates the tree's magic number
- Uses RT_SPAN to determine how many bits to process at each level
- The function assumes the tree has a valid root node (asserted at runtime)
- Part of the generic radixtree template system where different key/value types generate type-specific variants