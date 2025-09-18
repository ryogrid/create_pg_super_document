# rbt_iterate

## Location
[src/backend/lib/rbtree.c:826-832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L826-L832)

## Overview
Returns the next node in the tree traversal sequence, or NULL if the iteration is complete.

## Definition
```c
RBTNode *rbt_iterate(RBTreeIterator *iter)
```

## Detailed Description
This function advances the iterator to the next node in the traversal sequence and returns a pointer to that node. It serves as the main iteration function that should be called repeatedly after rbt_begin_iterate() until it returns NULL, indicating that all nodes have been visited. The function uses the iterator's internal state and function pointer (set by rbt_begin_iterate) to determine the next node according to the specified traversal order.

The function performs a simple check to see if the iteration is already complete (is_over flag) and returns NULL immediately if so. Otherwise, it delegates to the specific iterator function (either rbt_left_right_iterator or rbt_right_left_iterator) that was set during initialization.

## Parameters / Member Variables
- `iter`: Pointer to the RBTreeIterator structure containing the iteration state

## Dependencies
- Functions called/Symbols referenced:
  - [RBTreeIterator](../R/RBTreeIterator.md) (iterator state structure)
  - [RBTNode](../R/RBTNode.md) (return type - red-black tree node)
- Called from (representative examples):
  - [ginGetBAEntry](../g/ginGetBAEntry.md) (src/backend/access/gin/ginbulk.c:275)
  - testleftright (src/test/modules/test_rbtree/test_rbtree.c:174, 183)
  - testrightleft (src/test/modules/test_rbtree/test_rbtree.c:214, 223)

## Notes and Other Information
- Must be preceded by a call to rbt_begin_iterate() to initialize the iterator
- Returns NULL when no more nodes are available in the traversal
- The specific traversal order depends on the RBTOrderControl value passed to rbt_begin_iterate()
- Tree modifications during iteration result in unspecified behavior
- The returned RBTNode pointer should not be freed by the caller - it points to nodes owned by the tree structure