# RT_FREE

## Location
src/include/lib/radixtree.h: 2061 - 2093

## Overview
RT_FREE is a macro that expands to a function name for freeing and deallocating a radix tree data structure and all its associated memory.

## Definition
```c
#define RT_FREE RT_MAKE_NAME(free)
```

Function signature:
```c
RT_SCOPE void RT_FREE(RT_RADIX_TREE * tree);
```

## Detailed Description
RT_FREE is a preprocessor macro that generates a function name for freeing a radix tree. The macro uses the RT_MAKE_NAME helper to create a function name based on the configured prefix. The actual function performs complete cleanup of a radix tree data structure, including:

1. **Shared Memory Mode**: When RT_SHMEM is defined, it validates the tree's magic number, recursively frees all radix tree nodes using RT_FREE_RECURSE, vandalizes the control block by clearing the magic number, and frees the DSA (Dynamic Shared Area) handle.

2. **Regular Mode**: Resets the memory context associated with the tree, which automatically frees all memory allocated within that context and its child contexts.

The function ensures complete memory cleanup and helps prevent memory leaks by properly deallocating all resources associated with the radix tree.

## Parameters / Member Variables
- `tree`: Pointer to the RT_RADIX_TREE structure to be freed. Must be a valid, previously created radix tree.

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (for name generation)
  - [RT_FREE_RECURSE](RT_FREE_RECURSE.md) (in shared memory mode)
  - [MemoryContextReset](../M/MemoryContextReset.md) (in regular mode)
  - [dsa_free](../d/dsa_free.md) (in shared memory mode)
- Called from (representative examples):
  - User code that needs to clean up radix trees
  - Memory cleanup routines

## Notes and Other Information
- This is a template-generated function through macro expansion
- The actual function name depends on the RT_PREFIX configuration
- In shared memory mode, the function includes additional safety checks and DSA cleanup
- After calling RT_FREE, the tree pointer should not be used again
- The function is designed to be safe even if called multiple times (though not recommended)
- Part of PostgreSQL's generic radix tree implementation located in src/include/lib/radixtree.h:176