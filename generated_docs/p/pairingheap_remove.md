# pairingheap_remove

## Location
[src/backend/lib/pairingheap.c:170-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/pairingheap.c#L170-L233)

## Overview
Removes a specific node from anywhere within a pairing heap while maintaining the heap property and structure.

## Definition
```c
void pairingheap_remove(pairingheap *heap, pairingheap_node *node)
```

## Detailed Description
This function implements the general node removal operation for pairing heaps, allowing removal of any node (not just the root). It has O(log n) amortized time complexity. The algorithm handles two main cases:

1. **Root node removal**: If the target node is the root, it delegates to `pairingheap_remove_first()` for efficiency.

2. **Internal node removal**: For non-root nodes, it performs a more complex operation:
   - Saves references to the node's children and next sibling
   - Finds the pointer to this node in its parent or previous sibling
   - If the node has children, merges them into a subheap and links it in place of the removed node
   - If the node has no children, simply unlinks it by updating pointers
   - Updates all necessary parent/sibling relationships to maintain heap structure

The function carefully manages the doubly-linked structure of sibling nodes and the parent-child relationships to ensure heap integrity after removal.

## Parameters / Member Variables
- `heap`: Pointer to the pairing heap containing the node to remove
- `node`: Pointer to the specific node to remove from the heap

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_remove_first
  - merge_children
- Called from (representative examples):
  - ReorderBufferChangeMemoryUpdate
  - InvalidateCatalogSnapshot
  - UnregisterSnapshotNoOwner
  - AtEOXact_Snapshot

## Notes and Other Information
- Unlike `pairingheap_remove_first`, this function can remove any node from the heap
- The removed node is not freed; memory management is the caller's responsibility
- The function maintains heap structure by properly handling parent-child and sibling relationships
- Used in PostgreSQL's logical replication and snapshot management systems
- Critical for maintaining ordered collections where arbitrary elements need to be removed
- The algorithm ensures that heap ordering is preserved after removal by merging children appropriately