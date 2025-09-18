# sift_down

## Location
[src/common/binaryheap.c:313-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L313-L365)

## Overview
A static helper function that moves a node downward in the heap to maintain the heap property by comparing it with child nodes.

## Definition
```c
static void sift_down(binaryheap *heap, int node_off)
```

## Detailed Description
This function implements the "sift down" or "percolate down" operation in a binary heap, which moves a node toward the leaves until the heap property is satisfied. Like sift_up, it uses the hole-based optimization technique to minimize data copying operations.

The algorithm works as follows:
1. Stores the node value to be sifted in a temporary variable
2. Creates a conceptual "hole" at the nodes position
3. Iteratively compares the node value with its children (left and right)
4. Identifies which child (if any) violates the heap property
5. If both children are candidates, selects the one that better satisfies the heap ordering
6. Moves the selected child up into the hole and continues from the childs position
7. Stops when no heap property violations are found
8. Finally places the node value in its correct position

This function is more complex than sift_up because it must handle two children and determine which one to promote.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure  
- `node_off`: Zero-based index of the node to sift downward

## Dependencies
- Functions called/Symbols referenced:
  - [left_offset](../l/left_offset.md) (macro to calculate left child index)
  - [right_offset](../r/right_offset.md) (macro to calculate right child index)
  - heap->bh_compare (comparison function for heap ordering)
- Called from (representative examples):
  - [binaryheap_build](../b/binaryheap_build.md) (common/binaryheap.c:143)
  - [binaryheap_remove_first](../b/binaryheap_remove_first.md) (common/binaryheap.c:213)
  - [binaryheap_remove_node](../b/binaryheap_remove_node.md) (common/binaryheap.c:244)
  - [binaryheap_replace_first](../b/binaryheap_replace_first.md) (common/binaryheap.c:262)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Used when a node may violate the heap property by being "too small" for its position
- Must handle the case where a node has zero, one, or two children
- The hole-based optimization reduces assignment operations compared to naive swapping
- Time complexity is O(log n) in the worst case (height of the heap)
- Essential for maintaining heap invariants after removal operations and heap construction
- More complex than sift_up due to the need to compare and select between two potential children