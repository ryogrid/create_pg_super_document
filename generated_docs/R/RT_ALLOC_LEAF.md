# RT_ALLOC_LEAF

## Location
src/include/lib/radixtree.h: 897 - 919

## Overview
RT_ALLOC_LEAF is a macro that resolves to a function name for allocating new leaf nodes in PostgreSQL's generic radix tree implementation.

## Definition
```c
#define RT_ALLOC_LEAF RT_MAKE_NAME(alloc_leaf)

// The actual function signature:
static RT_CHILD_PTR RT_ALLOC_LEAF(RT_RADIX_TREE * tree, size_t allocsize)
```

## Detailed Description
RT_ALLOC_LEAF is part of PostgreSQL's templated radix tree implementation found in radixtree.h. This macro uses the RT_MAKE_NAME infrastructure to generate type-specific function names based on the RT_PREFIX defined when including the header. The actual function allocates memory for a new leaf node in the radix tree, handling both shared memory (RT_SHMEM) and regular memory contexts. The allocation size is specified by the caller and typically corresponds to the size needed to store the leaf's value data.

## Parameters / Member Variables
- `tree`: Pointer to the radix tree structure that will contain the new leaf
- `allocsize`: Size in bytes to allocate for the leaf node (includes space for value data)

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for name generation)
  - dsa_allocate (for shared memory allocation when RT_SHMEM is defined)
  - MemoryContextAlloc (for regular memory allocation)
  - RT_PTR_SET_LOCAL (for shared memory pointer setup)
- Called from (representative examples):
  - RT_SET (when storing values in the radix tree)

## Notes and Other Information
- The function increments the leaf count statistics when RT_DEBUG is enabled
- Memory allocation strategy depends on whether RT_SHMEM is defined (shared vs. local memory)
- Returns an RT_CHILD_PTR structure containing the allocated memory pointer
- This is part of a generic/templated implementation, so the actual function name varies based on the prefix used when instantiating the radix tree template
- The allocated leaf memory must be properly initialized by the caller before use