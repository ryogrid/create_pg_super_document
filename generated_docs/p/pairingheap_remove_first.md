# pairingheap_remove_first

## Location
[src/backend/lib/pairingheap.c:145-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/pairingheap.c#L145-L169)

## Overview
Removes the root (first, topmost) node from a pairing heap and returns a pointer to it after rebalancing the heap.

## Definition
```c
pairingheap_node *pairingheap_remove_first(pairingheap *heap)
```

## Detailed Description
This function implements the "remove minimum" operation for pairing heaps, which is a fundamental heap operation. It removes the root node (which contains the minimum element in a min-heap) and restructures the remaining heap to maintain the heap property. The operation has O(log n) amortized time complexity.

The algorithm works by:
1. Saving a reference to the root node to return later
2. Getting all children of the root node
3. Using the `merge_children` function to merge all children into a new heap
4. Setting the new root and clearing its parent/sibling pointers
5. Returning the original root node

The function includes an assertion to ensure it's not called on an empty heap, as this would be a programming error.

## Parameters / Member Variables
- `heap`: Pointer to the pairing heap from which to remove the first node

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - [merge_children](../m/merge_children.md)
- Called from (representative examples):
  - [getNextGISTSearchItem](../g/getNextGISTSearchItem.md)
  - [spgGetNextQueueItem](../s/spgGetNextQueueItem.md)  
  - [reorderqueue_pop](../r/reorderqueue_pop.md)
  - [pairingheap_remove](pairingheap_remove.md)

## Notes and Other Information
- The caller must ensure the heap is not empty before calling this function
- The returned node is not freed; memory management is the caller's responsibility
- This is the primary way to extract the minimum element from a pairing heap
- The function maintains heap integrity by properly restructuring after removal
- Used extensively in PostgreSQL's indexing and scanning operations where priority queues are needed