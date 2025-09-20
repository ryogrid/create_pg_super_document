# RBTree

## Location
[src/backend/lib/rbtree.c:41-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L41-L60)

## Overview
RBTree is a control structure that represents a generic Red-Black binary tree implementation in PostgreSQL, providing a balanced binary search tree with guaranteed O(log n) performance for search, insert, and delete operations.

## Definition

```c
struct RBTree
{
	RBTNode    *root;			/* root node, or RBTNIL if tree is empty */

	/* Remaining fields are constant after rbt_create */

	Size		node_size;		/* actual size of tree nodes */
	/* The caller-supplied manipulation functions */
	rbt_comparator comparator;
	rbt_combiner combiner;
	rbt_allocfunc allocfunc;
	rbt_freefunc freefunc;
	/* Passthrough arg passed to all manipulation functions */
	void	   *arg;
};
```
## Detailed Description
The RBTree structure serves as the main control structure for PostgreSQL's generic Red-Black tree implementation. Red-Black trees are self-balancing binary search trees that maintain balance through color coding (red/black) of nodes and specific balancing rules. This ensures that the longest path from root to leaf is at most twice as long as the shortest path, guaranteeing O(log n) time complexity for basic operations.

The structure is designed to be generic, allowing callers to embed RBTNode as the first field of larger structures containing application-specific data. The tree delegates key operations (comparison, allocation, deallocation, and data combination) to caller-provided function pointers, making it highly reusable across different data types and contexts.

The implementation is based on Thomas Niemann's "Sorting and Searching Algorithms: a Cookbook" and maintains the fundamental Red-Black tree properties: (1) any child of a red node is always black, and (2) every path from root to leaf traverses an equal number of black nodes.

## Parameters / Member Variables
- : Pointer to the root node of the tree, or RBTNIL if the tree is empty
- : The actual size in bytes of tree nodes (must be larger than sizeof(RBTNode)) to accommodate caller's additional data
- : Function pointer for comparing two RBTNodes, returns negative/zero/positive for less/equal/greater relationships
- : Function pointer for merging an existing tree entry with a new one during insertion when duplicates are found
- : Function pointer for allocating new RBTNode instances
- : Function pointer for deallocating RBTNode instances (can be NULL if retail space reclamation is not required)
- : Passthrough pointer argument that is passed to all manipulation functions for context

## Dependencies
- Functions called/Symbols referenced:
  - [RBTNode](RBTNode.md)
  - rbt_comparator
  - rbt_combiner
  - rbt_allocfunc
  - rbt_freefunc
- Called from (representative examples):
  - [rbt_create](../r/rbt_create.md)
  - [rbt_find](../r/rbt_find.md)
  - [rbt_insert](../r/rbt_insert.md)
  - [rbt_delete](../r/rbt_delete.md)
  - [rbt_begin_iterate](../r/rbt_begin_iterate.md)
  - [rbt_leftmost](../r/rbt_leftmost.md)

## Notes and Other Information
- The structure is opaque to callers - they should only access it through the provided API functions
- All tree contents are managed by the caller, not the tree implementation itself
- The tree uses a sentinel node (RBTNIL) to represent leaf nodes, which simplifies tree algorithms
- Memory management follows PostgreSQL's palloc/pfree patterns
- The combiner function's right-hand argument will be a "proposed" node where RBTNode fields may not be valid
- The freefunc should only handle node deallocation, not subsidiary data cleanup
- Typically destroyed by resetting or deleting the memory context rather than explicit cleanup
- [Node](../N/Node.md) size must be greater than sizeof(RBTNode) to accommodate caller's additional data fields