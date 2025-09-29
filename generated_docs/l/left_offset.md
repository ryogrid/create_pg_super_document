# left_offset

## Location
[src/common/binaryheap.c:90-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L90-L95)

## Overview
Calculates the array index of the left child node in a binary heap given the parent node's index.

## Definition
```c
static inline uint64 left_offset(uint64 i)
```

## Detailed Description
The `left_offset` function implements the standard binary heap indexing formula to find the left child of a node at index `i`. In a binary heap stored as an array, the left child of a node at index `i` is always located at index `2*i + 1`. This is a fundamental binary heap property that enables efficient parent-child navigation without requiring explicit pointer structures.

## Parameters / Member Variables
- `i`: Index of the parent node in the binary heap array

## Dependencies
- Functions called/Symbols referenced:
  - None (simple arithmetic operation)
- Called from (representative examples):
  - [ltsGetFreeBlock](ltsGetFreeBlock.md) (in logtape.c for heap operations)
  - [sift_down](../s/sift_down.md) (in binaryheap.c for heap property maintenance)

## Notes and Other Information
- This function is declared as `static inline` for optimal performance since it's a simple calculation used frequently in heap operations
- Returns `2 * i + 1` which is the standard formula for left child index in zero-based array representation of binary heaps
- Used in conjunction with `right_offset` for complete binary tree navigation
- Essential for heap maintenance operations like sifting down during heap property restoration
- The function works with 64-bit indices to support large heap sizes

## Simplified Source

```c
static inline uint64 left_offset(uint64 i) {
    // Calculate left child index in binary heap: 2*i + 1
    return 2 * i + 1;
}
```