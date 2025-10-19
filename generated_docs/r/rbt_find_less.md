# rbt_find_less

## Location
[src/backend/lib/rbtree.c:203-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L203-L234)

## Overview
Searches for the largest value in a Red-Black Tree that is less than or optionally equal to the provided data.

## Definition

```c
RBTNode *
rbt_find_less(RBTree *rbt, const RBTNode *data, bool equal_match)
```
## Detailed Description
This function performs a specialized search to find the node with the largest value that is less than the search data. If equal_match is true, it will also accept an exact match. The algorithm traverses the tree while maintaining a pointer to the best candidate found so far. When the comparison shows the search data is greater than the current node, that node becomes a candidate and the search continues right to find potentially larger candidates. When the search data is less than or equal to the current node, the search continues left to find smaller values.

## Parameters / Member Variables
- `*rbt`: Pointer to the RBTree structure to search in
- `*data`: Pointer to the data to compare against (RBTNode fields need not be valid)
- `equal_match`: Boolean flag - if true, the function will return exact matches; if false, only strictly lesser values are returned
## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (structure type)
  - [RBTNode](../R/RBTNode.md) (structure type)
  - RBTNIL (constant representing tree leaf/null)
  - comparator (function pointer from tree structure for comparing nodes)
- Called from (representative examples):
  - [testfindltgt](../t/testfindltgt.md) (test module function for verifying less-than search functionality)

## Notes and Other Information
- Returns the matching RBTNode pointer on success, or NULL if no qualifying match is found
- When equal_match is true, functions as a "less than or equal to" search
- When equal_match is false, functions as a "strictly less than" search
- The search maintains a 'lesser' variable to track the best candidate found during traversal
- Implements a modified binary search that tracks the closest lesser value
- Time complexity is O(log n) for balanced trees
- Useful for range queries and finding predecessor nodes in ordered traversals
- The data parameter's RBTNode fields don't need to be valid since only the embedded user data is used
- Complementary function to rbt_find_great, providing the opposite search direction

## Simplified Source

```c
RBTNode *
rbt_find_less(RBTree *rbt, const RBTNode *data, bool equal_match)
{
    RBTNode *node = rbt->root;
    RBTNode *lesser = NULL;  // Best candidate found so far

    while (node != RBTNIL)
    {
        int cmp = rbt->comparator(data, node, rbt->arg);

        if (equal_match && cmp == 0)
            return node;  // Exact match allowed and found
        else if (cmp > 0)
        {
            // Current node is lesser - save as candidate
            lesser = node;
            node = node->right;  // Look for larger candidates
        }
        else
            node = node->left;  // Need smaller values
    }

    return lesser;  // Return best candidate (or NULL)
}
```