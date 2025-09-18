# intset_memory_usage

## Location
src/backend/lib/integerset.c: 359 - 369

## Overview
Returns the total amount of memory used by an IntegerSet, including all allocated nodes and structures.

## Definition
```c
uint64 intset_memory_usage(IntegerSet *intset)
```

## Detailed Description
The `intset_memory_usage` function provides the total memory footprint of an IntegerSet in bytes. This includes the memory used by the main IntegerSet structure itself, all internal nodes, all leaf nodes, and any other allocated data structures. The memory usage is tracked incrementally as new nodes are allocated, making this function an O(1) operation that simply returns the cached value from the `mem_used` field.

## Parameters / Member Variables
- `intset`: Pointer to the IntegerSet structure whose memory usage is being queried

## Dependencies
- Functions called/Symbols referenced:
  - `[IntegerSet](../I/IntegerSet.md)`: Structure type being accessed
- Called from (representative examples):
  - Various test functions in test_integerset module for memory usage validation and testing

## Notes and Other Information
- This is a simple O(1) accessor function with no computational overhead
- The memory usage is automatically tracked during all allocation operations using `GetMemoryChunkSpace`
- Returns `uint64` to support large memory usage values
- The tracked memory includes all memory allocated in the IntegerSet's designated memory context
- Memory usage tracking begins when the IntegerSet is created and continues through all node allocations
- Useful for monitoring memory consumption and optimizing storage efficiency