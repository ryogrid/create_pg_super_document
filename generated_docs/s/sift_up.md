# sift_up

## Location
src/common/binaryheap.c: 270 - 312

## Overview
A static helper function that moves a node upward in the heap to maintain the heap property by comparing it with parent nodes.

## Definition
```c
static void sift_up(binaryheap *heap, int node_off)
```

## Detailed Description
This function implements the "sift up" or "bubble up" operation in a binary heap, which moves a node toward the root until the heap property is satisfied. It uses an optimization technique called "hole-based" sifting to minimize data copying operations.

The algorithm works as follows:
1. Stores the node value to be sifted in a temporary variable
2. Creates a conceptual "hole" at the nodes position
3. Iteratively compares the node value with parent nodes
4. If the node value is greater than a parent (should move up), moves the parent down into the hole
5. Continues until the node value is in the correct position or reaches the root
6. Finally places the node value in its final position

The hole-based approach avoids unnecessary data copying by only storing the final value once at the end, rather than swapping values at each step.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap structure
- `node_off`: Zero-based index of the node to sift upward

## Dependencies
- Functions called/Symbols referenced:
  - [parent_offset](../p/parent_offset.md) (macro to calculate parent node index)
  - heap->bh_compare (comparison function for heap ordering)
- Called from (representative examples):
  - [binaryheap_add](../b/binaryheap_add.md) (common/binaryheap.c:166)
  - [binaryheap_remove_node](../b/binaryheap_remove_node.md) (common/binaryheap.c:242)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Used when a node may violate the heap property by being "too large" for its position
- The hole-based optimization reduces the number of assignment operations
- Time complexity is O(log n) in the worst case (height of the heap)
- Essential for maintaining heap invariants after insertion or specific node removal operations
- The comparison direction depends on whether its a min-heap or max-heap