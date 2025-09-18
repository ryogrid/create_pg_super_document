# dlist_has_next

## Location
src/include/lib/ilist.h: 503 - 512

## Overview
Checks whether a given node in a doubly-linked list has a following (next) node, providing a safe way to determine if iteration can continue forward in the list.

## Definition
```c
static inline bool dlist_has_next(const dlist_head *head, const dlist_node *node)
```

## Detailed Description
This function determines whether a specified node has a next node in the doubly-linked list by comparing the node's next pointer with the list head's sentinel node. In PostgreSQL's doubly-linked list implementation, the list is circular with a sentinel head node, so the last node in the list points back to the head sentinel. The function returns `true` if the node's next pointer does not point to the head sentinel (meaning there is a following node), and `false` if it does point to the head sentinel (meaning this is the last node).

This function is commonly used in iteration loops to safely traverse the list without going past the end. However, the documentation warns that it's unreliable if the node is not actually part of the specified list, as the function doesn't validate membership.

## Parameters / Member Variables
- `head`: Pointer to the list head used to identify the end of the list
- `node`: Pointer to the node to check for a following node

## Dependencies
- Functions called/Symbols referenced:
  - dlist_head (type)
  - dlist_node (type)
- Called from (representative examples):
  - dataPlaceToPageLeafSplit
  - addItemsToLeaf
  - leafRepackItems
  - ReorderBufferIterTXNNext
  - pgstat_flush_pending_entries
  - BumpReset
  - GenerationReset
  - dlist_next_node
  - dclist_has_next

## Notes and Other Information
- This is an inline function defined in the header file for performance
- The function assumes the node is part of the specified list - results are unreliable if this assumption is violated
- Returns `true` if there is a following node, `false` if the current node is the last in the list
- Commonly used in PostgreSQL's GIN index operations, replication, statistics, and memory management systems
- The function leverages the circular nature of the doubly-linked list implementation where the last node points to the head sentinel
- No bounds checking or membership validation is performed - the caller must ensure the node belongs to the list
- Used extensively in iteration patterns where safe forward traversal is required
- Complementary to `dlist_has_prev` for bidirectional iteration support