# rbt_leftmost

## Location
[src/backend/lib/rbtree.c:235-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L235-L262)

## Overview
Returns the leftmost (smallest-valued) node in a Red-Black Tree, which represents the minimum element according to the tree's ordering.

## Definition

```c
RBTNode *
rbt_leftmost(RBTree *rbt)
```
## Detailed Description
This function traverses the Red-Black Tree to find the leftmost node, which contains the smallest value according to the tree's comparison function. The implementation follows the standard binary search tree property where the leftmost node is found by continuously following left child pointers until reaching a leaf (RBTNIL).

The function performs a simple iterative traversal starting from the root and moving left until it encounters RBTNIL. The algorithm maintains a pointer to the leftmost valid node encountered during traversal.

Note that this function only retrieves the leftmost node without unlinking it from the tree. If deletion is required, the caller should explicitly call rbt_delete on the returned node.

## Parameters / Member Variables
- `*rbt`: Pointer to the Red-Black Tree structure to search
## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (tree structure type)
  - [RBTNode](../R/RBTNode.md) (node structure type)  
  - RBTNIL (sentinel value for null nodes)
- Called from (representative examples):
  - [testleftmost](../t/testleftmost.md) (in test_rbtree.c:393, 400)
  - [testdelete](../t/testdelete.md) (in test_rbtree.c:488)

## Notes and Other Information
- Returns NULL if the tree is empty (root is RBTNIL)
- Time complexity is O(log n) where n is the number of nodes in the tree
- The original implementation included an unlink step, but this was removed to provide cleaner separation of concerns
- Commonly used in conjunction with rbt_delete when implementing operations that need to remove the minimum element
- The function is safe to call on empty trees

## Simplified Source

```c
RBTNode *
rbt_leftmost(RBTree *rbt)
{
    RBTNode *node = rbt->root;
    RBTNode *leftmost = rbt->root;

    // Keep going left to find minimum value
    while (node != RBTNIL)
    {
        leftmost = node;
        node = node->left;
    }

    // Return leftmost node, or NULL if tree is empty
    if (leftmost != RBTNIL)
        return leftmost;

    return NULL;
}
```