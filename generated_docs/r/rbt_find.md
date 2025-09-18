# rbt_find

## Location
[src/backend/lib/rbtree.c:145-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L145-L171)

## Overview
Searches for a value in a Red-Black Tree and returns the matching node if found.

## Definition


## Detailed Description
This function performs a binary search in the Red-Black Tree to locate a node that matches the provided data. It starts at the root and traverses the tree by comparing the search data with each node using the tree's comparator function. The search follows the binary search tree property: if the comparison result is less than zero, it goes left; if greater than zero, it goes right; if equal to zero, it returns the matching node. The function terminates when either a match is found or it reaches a leaf (RBTNIL).

## Parameters / Member Variables
- : Pointer to the RBTree structure to search in
- : Pointer to the data to search for (RBTNode fields need not be valid, only the embedded user data matters)

## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (structure type)
  - [RBTNode](../R/RBTNode.md) (structure type)
  - RBTNIL (constant representing tree leaf/null)
  - comparator (function pointer from tree structure for comparing nodes)
- Called from (representative examples):
  - testfind (test module function for verifying search functionality)
  - testdelete (test module function that searches before deletion)

## Notes and Other Information
- Returns the matching RBTNode pointer on success, or NULL if no match is found
- The data parameter's RBTNode fields don't need to be valid since only the embedded user data is used for comparison
- Uses the tree's configured comparator function to determine node ordering
- Implements standard binary search tree traversal algorithm
- Time complexity is O(log n) for balanced trees
- The search is read-only and does not modify the tree structure
- Comparison result interpretation: 0 = equal, < 0 = data is less than node, > 0 = data is greater than node