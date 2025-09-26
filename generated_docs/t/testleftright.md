# testleftright

## Location
src/test/modules/test_rbtree/test_rbtree.c: 164 - 203

## Overview
Validates the correctness of left-to-right (in-order) tree traversal by ensuring all elements are visited in strictly increasing order.

## Definition
```c
static void testleftright(int size)
```

## Detailed Description
testleftright performs comprehensive validation of the red-black tree's left-to-right traversal functionality. The function tests two critical scenarios:

1. **Empty Tree Validation**: Verifies that iterating over an empty tree produces no elements, ensuring the iterator handles edge cases correctly.

2. **Ordered Traversal Validation**: After populating the tree with consecutive natural numbers (0, 1, 2, ..., size-1) inserted in random order, the function validates that left-to-right traversal visits all elements in strictly increasing order.

The test is particularly important because red-black trees are binary search trees, and the fundamental property that in-order traversal should yield sorted order is essential for correctness. The function not only checks ordering but also ensures completeness (all elements visited) and correctness (proper start and end values).

## Parameters / Member Variables
- `size`: The number of elements to insert and traverse (creates sequence 0 through size-1)

## Dependencies
- Functions called/Symbols referenced:
  - create_int_rbtree (creates test red-black tree)
  - rbt_begin_iterate (initializes tree iterator for LeftRightWalk)
  - rbt_iterate (advances iterator and returns next node)
  - rbt_populate (populates tree with test data)
  - elog (PostgreSQL error logging)
  - LeftRightWalk (traversal direction constant)
  - IntRBTreeNode (test-specific node structure)
  - RBTreeIterator (tree iteration state structure)
- Called from (representative examples):
  - test_rb_tree (test_rbtree.c:509)

## Notes and Other Information
- Tests the fundamental binary search tree property that in-order traversal yields sorted sequence
- Validates both empty tree handling and populated tree traversal
- Uses strict inequality checks to ensure no duplicate values are encountered during traversal
- Performs completeness validation by counting visited elements and checking boundary values
- Essential component of red-black tree correctness validation
- The test assumes elements 0 through size-1, so lastKey should end at size-1 and count should equal size