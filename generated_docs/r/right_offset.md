# right_offset

## Location
[src/common/binaryheap.c:96-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L96-L101)

## Overview
Calculates the array index of the right child node in a binary heap given the parent node's index.

## Definition
```c
static inline uint64 right_offset(uint64 i)
```

## Detailed Description
The `right_offset` function implements the standard binary heap indexing formula to find the right child of a node at index `i`. In a binary heap stored as an array, the right child of a node at index `i` is always located at index `2*i + 2`. This is a fundamental binary heap property that enables efficient parent-child navigation without requiring explicit pointer structures, complementing the `left_offset` function for complete binary tree traversal.

## Parameters / Member Variables
- `i`: Index of the parent node in the binary heap array

## Dependencies
- Functions called/Symbols referenced:
  - None (simple arithmetic operation)
- Called from (representative examples):
  - ltsGetFreeBlock (in logtape.c for heap operations)
  - [sift_down](../s/sift_down.md) (in binaryheap.c for heap property maintenance)

## Notes and Other Information
- This function is declared as `static inline` for optimal performance since it's a simple calculation used frequently in heap operations
- Returns `2 * i + 2` which is the standard formula for right child index in zero-based array representation of binary heaps
- Used in conjunction with `left_offset` for complete binary tree navigation
- Essential for heap maintenance operations like sifting down during heap property restoration
- The function works with 64-bit indices to support large heap sizes
- Together with `left_offset`, provides the complete set of child navigation functions for binary heap algorithms