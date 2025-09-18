# rbt_insert_fixup

## Location
[src/backend/lib/rbtree.c:344-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L344-L452)

## Overview
Restores Red-Black Tree balance properties after inserting a new node, using color changes and rotations to maintain the invariants.

## Definition


## Detailed Description
This function is responsible for maintaining Red-Black Tree balance after a node insertion. Since new nodes are always inserted as red, this may create violations where a red node has a red parent. The function systematically resolves these violations through a combination of color changes and tree rotations.

The algorithm operates on the principle that violations can always be resolved by moving the problem progressively higher up the tree until either:
1. The violation reaches the root (easily fixed by coloring the root black)
2. A configuration is reached where rotations can definitively resolve the issue

The function handles two symmetric cases based on whether the problematic node's parent is a left or right child. For each case, it considers whether the "uncle" node (parent's sibling) is red or black, leading to different resolution strategies:

- **Red uncle**: Perform color flips and move the violation up the tree
- **Black uncle**: Perform rotations and recoloring to definitively resolve the violation

## Parameters / Member Variables
- : Pointer to the Red-Black Tree structure
- : The newly inserted red node that may be causing Red-Black Tree violations

## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (tree structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)
  - RBTRED, RBTBLACK (color constants)
  - color (node color field)
  - [rbt_rotate_left](rbt_rotate_left.md), rbt_rotate_right (rotation operations)
- Called from (representative examples):
  - [rbt_insert](rbt_insert.md) (in rbtree.c:508)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- Time complexity is O(log n) in the worst case, but often terminates much earlier
- The function assumes x is initially a red node
- Maintains all Red-Black Tree invariants:
  - Every node is either red or black
  - The root is always black
  - Red nodes cannot have red children
  - All paths from root to leaves have equal black-height
- The algorithm is based on the classic Red-Black Tree insertion fixup procedure
- Critical for maintaining the O(log n) performance guarantees of Red-Black Trees
- Always terminates by ensuring the root is black, which may increase the tree's black-height by one