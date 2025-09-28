# pairingheap_first

## Location
[src/backend/lib/pairingheap.c:130-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/pairingheap.c#L130-L144)

## Overview
Returns a pointer to the root (topmost) node of the pairing heap without modifying the heap structure, providing O(1) access to the minimum/maximum element.

## Definition
```c
pairingheap_node *pairingheap_first(pairingheap *heap)
```

## Detailed Description
The `pairingheap_first` function provides constant-time access to the first (root) node of the pairing heap, which contains the minimum or maximum element depending on the comparison function used when the heap was created. This is a read-only operation that does not modify the heap structure in any way.

The function includes an assertion to ensure that the heap is not empty before attempting to access the root node. This is critical because accessing the root of an empty heap would result in returning a NULL pointer, which could lead to undefined behavior if not properly handled by the caller.

This function is essential for priority queue operations where you need to examine the highest-priority element before deciding whether to remove it or perform other operations.

## Parameters / Member Variables
- `heap`: Pointer to the pairing heap structure from which to retrieve the first node

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty (checks if heap is empty)
  - [pairingheap](pairingheap.md) (heap structure type)
  - [pairingheap_node](pairingheap_node.md) (node structure type)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [IndexNextWithReorder](../I/IndexNextWithReorder.md) (index scanning with reordering)
  - [ReorderBufferLargestTXN](../R/ReorderBufferLargestTXN.md) (logical replication transaction management)
  - [GetOldestSnapshot](../G/GetOldestSnapshot.md) (transaction snapshot management)
  - [SnapshotResetXmin](../S/SnapshotResetXmin.md) (snapshot transaction ID management)

## Notes and Other Information
- Provides O(1) access time to the root element, a key advantage of heap data structures
- Does not modify the heap - purely a read operation
- The caller must ensure the heap is not empty before calling this function
- Returns a pointer to the actual node, not a copy of the data
- Commonly used in priority queue scenarios where you need to peek at the next item to process
- Used extensively in PostgreSQL's indexing and transaction management systems
- The returned node pointer can be used to access the actual data stored in the node

## Simplified Source

```c
// Simplified version of pairingheap_first
pairingheap_node *pairingheap_first(pairingheap *heap) {
    Assert(!pairingheap_is_empty(heap));

    return heap->ph_root;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Maintained the essential empty heap assertion
- Focused on the core operation: returning the root node
- Preserved the O(1) access pattern