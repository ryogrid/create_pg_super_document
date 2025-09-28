# pairingheap_add

## Location
[src/backend/lib/pairingheap.c:112-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/pairingheap.c#L112-L129)

## Overview
Adds a new node to a pairing heap in O(1) time by merging it with the current root.

## Definition
```c
void pairingheap_add(pairingheap *heap, pairingheap_node *node)
```

## Detailed Description
The `pairingheap_add` function inserts a new node into the pairing heap data structure. The operation is performed in constant O(1) time, which is one of the key advantages of pairing heaps. The function works by treating the new node as a single-node subtree and merging it with the current root using the internal `merge` function.

The function first initializes the new node by setting its `first_child` pointer to NULL, then merges the new node with the existing root. After the merge, the resulting root has its sibling and parent pointers properly set to NULL since it becomes the new root of the entire heap.

This simple insertion strategy maintains the heap property and the structural invariants of the pairing heap, making it efficient for building priority queues.

## Parameters / Member Variables
- `heap`: Pointer to the pairing heap structure where the node will be added
- `node`: Pointer to the pairing heap node to be inserted into the heap

## Dependencies
- Functions called/Symbols referenced:
  - [merge](../m/merge.md) (internal merge function for combining subtrees)
  - [pairingheap](pairingheap.md) (heap structure type)
  - [pairingheap_node](pairingheap_node.md) (node structure type)
- Called from (representative examples):
  - [gistScanPage](../g/gistScanPage.md) (GiST index page scanning)
  - [spgAddSearchItemToQueue](../s/spgAddSearchItemToQueue.md) (SP-GiST search queue management)
  - [reorderqueue_push](../r/reorderqueue_push.md) (index scan reorder queue)
  - [ReorderBufferChangeMemoryUpdate](../R/ReorderBufferChangeMemoryUpdate.md) (logical replication memory management)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md) (transaction snapshot management)
  - [RegisterSnapshotOnOwner](../R/RegisterSnapshotOnOwner.md) (snapshot registration)

## Notes and Other Information
- Provides O(1) insertion time, a key advantage of pairing heaps over binary heaps
- The node's `first_child` is set to NULL as it starts as a leaf
- After insertion, the new root has proper NULL sibling and parent pointers
- Widely used throughout PostgreSQL for priority queue operations in indexing, scanning, and transaction management
- The caller is responsible for allocating and initializing the node's data before calling this function
- Does not perform any memory allocation - only manipulates existing node pointers

## Simplified Source

```c
// Simplified version of pairingheap_add
void pairingheap_add(pairingheap *heap, pairingheap_node *node) {
    // Initialize the new node as a leaf (no children)
    node->first_child = NULL;

    // Merge the new node with the current root
    heap->ph_root = merge(heap, heap->ph_root, node);

    // Set root pointers properly (no parent or siblings for root)
    heap->ph_root->prev_or_parent = NULL;
    heap->ph_root->next_sibling = NULL;
}
```

Key simplifications made:
- Added clear comments explaining each step
- Preserved the essential O(1) insertion logic
- Maintained the critical pointer manipulations
- Simple and straightforward implementation that matches the original's efficiency