# BTStackData

## Location
src/include/access/nbtree.h: 732 - 737

## Overview
BTStackData is a structure used to maintain a private stack during B-tree traversal, storing the locations of pivot tuples whose downlinks are followed during tree descent.

## Definition


## Detailed Description
BTStackData implements a linked-list stack structure that tracks the path taken during B-tree descent. As the tree traversal algorithm descends the tree, it pushes the location of pivot tuples onto this private stack before following their downlinks. This stack serves a crucial role during leaf page splits - it provides the necessary information to walk back up the tree and insert data into parent pages at the correct locations. The stack also handles recursive insertions when parent pages themselves need to split.

The structure is designed to be resilient to concurrent operations. While the stack can become stale due to concurrent page splits and deletions occurring during traversal, it is designed to never provide an irredeemably incorrect view of the tree structure.

## Parameters / Member Variables
- : Block number of the page containing the pivot tuple
- : Offset number within the page where the pivot tuple is located
- : Pointer to the parent entry in the stack, forming a linked list structure

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type)
  - OffsetNumber (type)
- Called from (representative examples):
  - _bt_insert_parent
  - _bt_search
  - BTStack (typedef alias)

## Notes and Other Information
- The stack is implemented as a singly-linked list with the  pointer linking to higher levels in the tree
- This structure is essential for maintaining consistency during concurrent B-tree operations
- The stack may become stale but should remain usable for tree navigation and modification operations
- Used primarily during insertion operations that may require upward propagation of changes