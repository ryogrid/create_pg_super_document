# RT_MEMORY_USAGE

## Location
[src/include/lib/radixtree.h:2688-2705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L2688-L2705)

## Overview
RT_MEMORY_USAGE is a macro that generates the name for a function that returns the total memory usage statistics of a radix tree data structure in PostgreSQL.

## Definition
```c
#define RT_MEMORY_USAGE RT_MAKE_NAME(memory_usage)
```

The actual function signature when expanded:
```c
RT_SCOPE uint64 RT_MEMORY_USAGE(RT_RADIX_TREE *tree)
```

## Detailed Description
RT_MEMORY_USAGE provides a standardized interface for retrieving memory usage statistics from PostgreSQL's radix tree implementation. This function returns the total amount of memory allocated by the radix tree, measured in bytes. 

The implementation varies depending on whether the tree is using shared memory (RT_SHMEM) or regular memory contexts. For shared memory trees, it uses the Dynamic Shared Area (DSA) interface to get total size information. For regular memory context trees, it queries the memory context directly to determine allocated memory including child contexts.

The function is designed to be thread-safe for shared memory scenarios since dsa_get_total_size() provides appropriate internal locking, eliminating the need for external locking by the caller.

## Parameters / Member Variables
- `tree`: Pointer to the RT_RADIX_TREE structure for which to calculate memory usage

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - [dsa_get_total_size](../d/dsa_get_total_size.md) (for shared memory trees - gets total DSA size)
  - MemoryContextMemAllocated (for regular trees - gets memory context allocation)
  - Assert (debugging assertion macro)

- Called from (representative examples):
  - RT_HANDLE (radixtree handle operations)
  - Memory monitoring and debugging code
  - Performance analysis routines

## Notes and Other Information
- Returns a uint64 value representing total memory usage in bytes
- No external locking required when called on shared memory trees due to internal DSA locking
- For regular memory contexts, includes memory from child contexts when second parameter is true
- Includes magic number validation for shared memory trees to ensure data structure integrity
- Part of PostgreSQL's generic radix tree template system
- Primarily used for monitoring, debugging, and performance analysis of radix tree memory consumption
- The function provides a unified interface regardless of whether the tree uses shared memory or regular memory contexts