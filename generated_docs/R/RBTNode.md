# RBTNode

## Location
[src/include/lib/rbtree.h:23-29](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/rbtree.h#L23-L29)

## Overview
RBTNode is the fundamental node structure for PostgreSQL's red-black tree implementation, designed to be embedded as the first field of larger structs that carry application-specific payload data.

## Definition

```c
typedef struct RBTNode
{
	char color;					/* node's current color, red or black */
	struct RBTNode *left;		/* left child, or RBTNIL if none */
	struct RBTNode *right;		/* right child, or RBTNIL if none */
	struct RBTNode *parent;		/* parent, or NULL (not RBTNIL!) if none */
} RBTNode;
```
## Detailed Description
RBTNode serves as the base structure for all nodes in PostgreSQL's red-black tree implementation. It follows a composition pattern where applications define larger structs with RBTNode as the first field, allowing the tree operations to work with the RBTNode portion while applications access their specific data through pointer casting. The structure maintains the essential red-black tree properties: node color for balancing and three pointers for tree navigation. This design provides type safety while allowing flexibility for different data types to be stored in the tree.

## Parameters / Member Variables
- `color`: A character field storing the node's color (red or black) used by red-black tree balancing algorithms
- `*left`: Pointer to the left child node, or RBTNIL sentinel value if no left child exists
- `*right`: Pointer to the right child node, or RBTNIL sentinel value if no right child exists
- `*parent`: Pointer to the parent node, or NULL if this is the root node (note: uses NULL, not RBTNIL)
## Dependencies
- Functions called/Symbols referenced:
  - color (member access)
  - [RBTNode](RBTNode.md) (self-referential pointers)
- Called from (representative examples):
  - [RBTree](RBTree.md) (tree structure definition)
  - [rbt_create](../r/rbt_create.md) (tree creation)
  - [rbt_find](../r/rbt_find.md) (node searching)
  - [rbt_insert](../r/rbt_insert.md) (node insertion)
  - [rbt_delete_node](../r/rbt_delete_node.md) (node deletion)
  - [rbt_rotate_left](../r/rbt_rotate_left.md) (tree rotation)
  - [rbt_rotate_right](../r/rbt_rotate_right.md) (tree rotation)
  - [GinEntryAccumulator](../G/GinEntryAccumulator.md) (GIN index usage)
  - [IntRBTreeNode](../I/IntRBTreeNode.md) (test module usage)

## Notes and Other Information
- [RBTNode](RBTNode.md) must be treated as an opaque structure by callers - direct manipulation of its fields should be avoided
- The structure is designed for composition: applications should define structs with RBTNode as the first field
- Parent pointers use NULL for root nodes, while child pointers use RBTNIL sentinel for missing children
- The design enables efficient tree operations while maintaining type safety for application-specific data
- Used extensively in GIN indexing and various internal PostgreSQL data structures requiring balanced tree access