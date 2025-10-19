# rbt_find_great

## Location
[src/backend/lib/rbtree.c:172-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L172-L202)

## Overview
Searches for the smallest value in a Red-Black Tree that is greater than or optionally equal to the provided data.

## Definition

```c
RBTNode *
rbt_find_great(RBTree *rbt, const RBTNode *data, bool equal_match)
```
## Detailed Description
This function performs a specialized search to find the node with the smallest value that is greater than the search data. If equal_match is true, it will also accept an exact match. The algorithm traverses the tree while maintaining a pointer to the best candidate found so far. When the comparison shows the search data is less than the current node, that node becomes a candidate and the search continues left to find potentially smaller candidates. When the search data is greater than or equal to the current node, the search continues right to find larger values.

## Parameters / Member Variables
- `*rbt`: Pointer to the RBTree structure to search in
- `*data`: Pointer to the data to compare against (RBTNode fields need not be valid)
- `equal_match`: Boolean flag - if true, the function will return exact matches; if false, only strictly greater values are returned
## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (structure type)
  - [RBTNode](../R/RBTNode.md) (structure type)
  - RBTNIL (constant representing tree leaf/null)
  - comparator (function pointer from tree structure for comparing nodes)
- Called from (representative examples):
  - [testfindltgt](../t/testfindltgt.md) (test module function for verifying greater-than search functionality)

## Notes and Other Information
- Returns the matching RBTNode pointer on success, or NULL if no qualifying match is found
- When equal_match is true, functions as a "greater than or equal to" search
- When equal_match is false, functions as a "strictly greater than" search
- The search maintains a 'greater' variable to track the best candidate found during traversal
- Implements a modified binary search that tracks the closest greater value
- Time complexity is O(log n) for balanced trees
- Useful for range queries and finding successor nodes in ordered traversals
- The data parameter's RBTNode fields don't need to be valid since only the embedded user data is used

## Simplified Source

```c
RBTNode *
rbt_find_great(RBTree *rbt, const RBTNode *data, bool equal_match)
{
    RBTNode *node = rbt->root;
    RBTNode *greater = NULL;  // Best candidate found so far

    while (node != RBTNIL)
    {
        int cmp = rbt->comparator(data, node, rbt->arg);

        if (equal_match && cmp == 0)
            return node;  // Exact match allowed and found
        else if (cmp < 0)
        {
            // Current node is greater - save as candidate
            greater = node;
            node = node->left;  // Look for smaller candidates
        }
        else
            node = node->right;  // Need larger values
    }

    return greater;  // Return best candidate (or NULL)
}
```