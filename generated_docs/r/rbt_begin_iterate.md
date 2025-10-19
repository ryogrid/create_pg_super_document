# rbt_begin_iterate

## Location
[src/backend/lib/rbtree.c:802-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L802-L825)

## Overview
Initializes an RBTreeIterator structure to prepare for traversing a red-black tree in a specified order (left-right or right-left).

## Definition

```c
void
rbt_begin_iterate(RBTree *rbt, RBTOrderControl ctrl, RBTreeIterator *iter)
```
## Detailed Description
This function sets up an iterator state for traversing a red-black tree. It initializes the RBTreeIterator structure with the appropriate iterator function pointer based on the requested traversal order. The function supports two traversal orders: LeftRightWalk (in-order: left, self, right) and RightLeftWalk (reverse in-order: right, self, left). After calling this function, the caller should repeatedly call rbt_iterate() to get successive nodes until NULL is returned.

The iterator maintains internal state to track the current position in the tree traversal. If the tree is modified during iteration, the behavior of subsequent rbt_iterate() calls becomes undefined. However, multiple concurrent iterators on the same tree are supported.

## Parameters / Member Variables
- `*rbt`: Pointer to the RBTree structure to iterate over
- `ctrl`: RBTOrderControl enum value specifying the traversal order (LeftRightWalk or RightLeftWalk)
- `*iter`: Pointer to RBTreeIterator structure that will be initialized with iteration state
## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (tree structure)
  - RBTOrderControl (traversal order enum)
  - [RBTreeIterator](../R/RBTreeIterator.md) (iterator state structure)
  - RBTNIL (sentinel node constant)
  - LeftRightWalk (traversal order constant)
  - [rbt_left_right_iterator](rbt_left_right_iterator.md) (left-right iterator function)
  - RightLeftWalk (traversal order constant) 
  - [rbt_right_left_iterator](rbt_right_left_iterator.md) (right-left iterator function)
  - elog (error logging function)
- Called from (representative examples):
  - [ginBeginBAScan](../g/ginBeginBAScan.md) (src/backend/access/gin/ginbulk.c:259)
  - [testleftright](../t/testleftright.md) (src/test/modules/test_rbtree/test_rbtree.c:173, 181)
  - [testrightleft](../t/testrightleft.md) (src/test/modules/test_rbtree/test_rbtree.c:213, 221)

## Notes and Other Information
- The iterator state is stored in an opaque RBTreeIterator struct that callers should not modify directly
- Tree modifications during traversal result in unspecified behavior for subsequent iterations
- Multiple concurrent iterators on the same tree are supported and safe
- The function will error out with elog(ERROR) if an unrecognized traversal order is specified
- The iterator is considered "over" immediately if the tree root is RBTNIL (empty tree)

## Simplified Source

```c
void
rbt_begin_iterate(RBTree *rbt, RBTOrderControl ctrl, RBTreeIterator *iter)
{
    // Initialize common iterator state
    iter->rbt = rbt;
    iter->last_visited = NULL;
    iter->is_over = (rbt->root == RBTNIL);  // Empty tree check

    // Set appropriate iterator function based on traversal order
    switch (ctrl) {
        case LeftRightWalk:    // In-order: left, self, right
            iter->iterate = rbt_left_right_iterator;
            break;
        case RightLeftWalk:    // Reverse order: right, self, left
            iter->iterate = rbt_right_left_iterator;
            break;
        default:
            elog(ERROR, "unrecognized rbtree iteration order: %d", ctrl);
    }
}
```