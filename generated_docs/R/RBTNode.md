# RBTNode

## Location
src/include/lib/rbtree.h: 23 - 29

## Overview
RBTNode is the fundamental node structure for PostgreSQL's red-black tree implementation, designed to be embedded as the first field of larger structs that carry application-specific payload data.

## Definition


## Detailed Description
RBTNode serves as the base structure for all nodes in PostgreSQL's red-black tree implementation. It follows a composition pattern where applications define larger structs with RBTNode as the first field, allowing the tree operations to work with the RBTNode portion while applications access their specific data through pointer casting. The structure maintains the essential red-black tree properties: node color for balancing and three pointers for tree navigation. This design provides type safety while allowing flexibility for different data types to be stored in the tree.

## Parameters / Member Variables
- : A character field storing the node's color (red or black) used by red-black tree balancing algorithms
- : Pointer to the left child node, or RBTNIL sentinel value if no left child exists
- : Pointer to the right child node, or RBTNIL sentinel value if no right child exists  
- : Pointer to the parent node, or NULL if this is the root node (note: uses NULL, not RBTNIL)

## Dependencies
- Functions called/Symbols referenced:
  - color (member access)
  - RBTNode (self-referential pointers)
- Called from (representative examples):
  - RBTree (tree structure definition)
  - rbt_create (tree creation)
  - rbt_find (node searching)
  - rbt_insert (node insertion)
  - rbt_delete_node (node deletion)
  - rbt_rotate_left (tree rotation)
  - rbt_rotate_right (tree rotation)
  - GinEntryAccumulator (GIN index usage)
  - IntRBTreeNode (test module usage)

## Notes and Other Information
- RBTNode must be treated as an opaque structure by callers - direct manipulation of its fields should be avoided
- The structure is designed for composition: applications should define structs with RBTNode as the first field
- Parent pointers use NULL for root nodes, while child pointers use RBTNIL sentinel for missing children
- The design enables efficient tree operations while maintaining type safety for application-specific data
- Used extensively in GIN indexing and various internal PostgreSQL data structures requiring balanced tree access