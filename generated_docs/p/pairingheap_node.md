# pairingheap_node

## Location
[src/include/lib/pairingheap.h:30-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/pairingheap.h#L30-L35)

## Overview
A fundamental data structure node that represents an element in a pairing heap, designed to be embedded within larger structs containing the actual data being stored.

## Definition

```c
typedef struct pairingheap_node
{
	struct pairingheap_node *first_child;
	struct pairingheap_node *next_sibling;
	struct pairingheap_node *prev_or_parent;
} pairingheap_node;
```
## Detailed Description
The  structure implements the node representation for PostgreSQL's pairing heap data structure. It uses a tree-like structure where each node can have multiple children organized as a doubly-linked list. This design allows efficient heap operations while maintaining the heap property.

The node structure supports the pairing heap's characteristic operations: merging, insertion, and deletion. The three pointer fields create a hybrid tree/list structure that enables the pairing heap algorithm to efficiently maintain heap ordering and perform merge operations.

The node is designed to be embedded within larger application-specific structures rather than used standalone, following PostgreSQL's pattern of intrusive data structures for memory efficiency.

## Parameters / Member Variables
- : Points to the node's first child in the heap tree structure, or NULL if the node has no children
- : Points to the next sibling node in the doubly-linked list of children, or NULL if this is the last child
- : Points to either the previous sibling node (if this node is not the first child) or to the parent node (if this node is the first child of its parent)

## Dependencies
- Functions called/Symbols referenced: None (pure data structure)
- Called from (representative examples):
  -  (for inserting nodes)
  -  (for removing nodes)
  -  (for extracting the root)
  -  (for merging heap subtrees)
  -  (for merging child lists)
  -  (in index scan execution)
  -  (in GIST index searching)
  -  (in SP-GIST index searching)

## Notes and Other Information
- This structure is intended to be embedded within larger structs using the  macro to recover the containing structure from a node pointer
- The dual-purpose nature of  (pointing to either previous sibling or parent) is a space-saving optimization that requires careful handling in heap algorithms
- Used extensively throughout PostgreSQL for priority queues in index scanning, replication logic, and snapshot management
- The node structure supports both const and non-const access patterns through corresponding container macros
- Memory management is the responsibility of the containing structure, not the node itself