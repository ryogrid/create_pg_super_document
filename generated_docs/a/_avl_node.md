# _avl_node

## Location
src/bin/psql/crosstabview.c: 50 - 66

## Overview
The _avl_node structure represents a node in an AVL (Adelson-Velsky and Landis) balanced binary search tree used for organizing pivot fields in psql's crosstabview functionality.

## Definition


## Detailed Description
The _avl_node structure is the fundamental building block of an AVL tree implementation used specifically for managing pivot fields in PostgreSQL's psql crosstabview feature. AVL trees are self-balancing binary search trees that maintain logarithmic time complexity for insertion, deletion, and search operations by ensuring the tree remains balanced.

Each node contains a pivot_field with the actual data, height information for balancing calculations, and pointers to left and right child nodes. The children array uses a convention where children[0] points to the left subtree and children[1] points to the right subtree. When a child doesn't exist, the pointer references a special empty node (avl_tree.end) rather than NULL, which simplifies tree traversal algorithms.

## Parameters / Member Variables
- : The pivot_field data contained in this node, representing a distinct value from the crosstab headers
- : The height of this node in the tree, calculated as the number of nodes on the longest path to a leaf; used for AVL balancing operations  
- : Array of child node pointers where children[0] is the left subtree and children[1] is the right subtree; points to avl_tree.end when no child exists rather than NULL

## Dependencies
- Functions called/Symbols referenced:
  - pivot_field (as member data type)
  - _avl_node (self-reference for child pointers)
- Called from (representative examples):
  - _avl_tree (as node type in tree structure)
  - avl_tree (in tree operations and management)
  - avlInit (for tree initialization)
  - avlFree (for memory cleanup)
  - avlUpdateHeight (for balancing operations)
  - avlRotate (for tree rotations)
  - avlBalance (for maintaining AVL properties)
  - avlInsertNode (for node insertion)
  - avlCollectFields (for tree traversal)

## Notes and Other Information
- Part of psql's crosstabview AVL tree implementation located in src/bin/psql/crosstabview.c:50-66
- Uses the AVL tree data structure to maintain sorted pivot fields with O(log n) operations
- The children array design avoids NULL checks by using a sentinel empty node
- Height field is crucial for AVL balancing algorithms that ensure tree performance
- The typedef creates the alias 'avl_node' for easier usage throughout the AVL tree implementation
- Self-referential structure allowing for recursive tree operations and traversals