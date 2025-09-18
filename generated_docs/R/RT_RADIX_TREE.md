# RT_RADIX_TREE

## Location
src/include/lib/radixtree.h: 707 - 732

## Overview
RT_RADIX_TREE is a macro that generates a type name for the main adaptive radix tree structure in PostgreSQL's templated radix tree implementation, serving as the primary API entry point.

## Definition


## Detailed Description
RT_RADIX_TREE is the main entry point structure for PostgreSQL's adaptive radix tree implementation. This structure provides a high-level interface that abstracts away the differences between local memory and shared memory deployments, while managing memory allocation contexts for different components of the tree.

The structure serves as a facade that contains the core control structure (RT_RADIX_TREE_CONTROL) and manages memory allocation through either traditional PostgreSQL memory contexts (for local trees) or Dynamic Shared Areas (DSA) for shared memory trees. This design allows the same radix tree implementation to work efficiently in both single-process and multi-process scenarios.

The memory management strategy is sophisticated, using separate memory contexts for different types of allocations: node slabs organized by size class for internal nodes, a dedicated leaf context for single-value leaves, and an iterator context for traversal operations. This separation enables more efficient memory allocation and cleanup patterns.

## Parameters / Member Variables
- : Main memory context for the tree structure itself
- : Pointer to the core control structure containing tree state and metadata
- : (RT_SHMEM only) Dynamic Shared Area for shared memory allocation
- : (Local memory only) Array of memory contexts, one for each node size class
- : (Local memory only) Memory context specifically for single-value leaf allocations
- : Memory context for iterator state and temporary data during tree traversal

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX
  - RT_PREFIX
  - [RT_RADIX_TREE_CONTROL](RT_RADIX_TREE_CONTROL.md)
  - [MemoryContext](../M/MemoryContext.md)
  - [MemoryContextData](../M/MemoryContextData.md)
  - dsa_area (in shared memory mode)
  - RT_NUM_SIZE_CLASSES
- Called from (representative examples):
  - RT_CREATE
  - [RT_ATTACH](RT_ATTACH.md)
  - [RT_DETACH](RT_DETACH.md)
  - [RT_FREE](RT_FREE.md)
  - [RT_FIND](RT_FIND.md)
  - [RT_SET](RT_SET.md)
  - [RT_DELETE](RT_DELETE.md)
  - [RT_BEGIN_ITERATE](RT_BEGIN_ITERATE.md)
  - [RT_LOCK_EXCLUSIVE](RT_LOCK_EXCLUSIVE.md)
  - [RT_LOCK_SHARE](RT_LOCK_SHARE.md)
  - [RT_UNLOCK](RT_UNLOCK.md)
  - [RT_GET_HANDLE](RT_GET_HANDLE.md)
  - [RT_MEMORY_USAGE](RT_MEMORY_USAGE.md)
  - [RT_STATS](RT_STATS.md)

## Notes and Other Information
The RT_RADIX_TREE structure demonstrates several important design principles:

1. **Memory Management Abstraction**: The structure elegantly handles two completely different memory allocation strategies (local vs shared) behind a unified interface, making the tree usable in various PostgreSQL contexts.

2. **Size Class Optimization**: The node_slabs array provides dedicated memory contexts for each of the five node size classes, enabling more efficient allocation patterns and reducing memory fragmentation.

3. **Context Separation**: Different types of data (nodes, leaves, iterators) use separate memory contexts, allowing for optimized allocation strategies and simplified cleanup when the tree is destroyed.

4. **Shared Memory Support**: The conditional compilation allows the same codebase to support both traditional PostgreSQL memory contexts and DSA-based shared memory allocation, crucial for scenarios like parallel query processing.

5. **Iterator Management**: The dedicated iter_context enables multiple concurrent iterators over the same tree without interfering with the tree's core memory management.

This structure is the user-visible handle returned by RT_CREATE() and passed to all other radix tree operations, making it the central API anchor point for the entire radix tree system.