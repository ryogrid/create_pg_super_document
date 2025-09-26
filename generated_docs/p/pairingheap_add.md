# pairingheap_add

## Location
src/backend/lib/pairingheap.c: 112 - 129

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
  - merge (internal merge function for combining subtrees)
  - pairingheap (heap structure type)
  - pairingheap_node (node structure type)
- Called from (representative examples):
  - gistScanPage (GiST index page scanning)
  - spgAddSearchItemToQueue (SP-GiST search queue management)
  - reorderqueue_push (index scan reorder queue)
  - ReorderBufferChangeMemoryUpdate (logical replication memory management)
  - GetTransactionSnapshot (transaction snapshot management)
  - RegisterSnapshotOnOwner (snapshot registration)

## Notes and Other Information
- Provides O(1) insertion time, a key advantage of pairing heaps over binary heaps
- The node's `first_child` is set to NULL as it starts as a leaf
- After insertion, the new root has proper NULL sibling and parent pointers
- Widely used throughout PostgreSQL for priority queue operations in indexing, scanning, and transaction management
- The caller is responsible for allocating and initializing the node's data before calling this function
- Does not perform any memory allocation - only manipulates existing node pointers