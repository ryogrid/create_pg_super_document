# testleftright

## Location
[src/test/modules/test_rbtree/test_rbtree.c:164-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L164-L203)

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
  - [create_int_rbtree](../c/create_int_rbtree.md) (creates test red-black tree)
  - [rbt_begin_iterate](../r/rbt_begin_iterate.md) (initializes tree iterator for LeftRightWalk)
  - [rbt_iterate](../r/rbt_iterate.md) (advances iterator and returns next node)
  - [rbt_populate](../r/rbt_populate.md) (populates tree with test data)
  - elog (PostgreSQL error logging)
  - LeftRightWalk (traversal direction constant)
  - [IntRBTreeNode](../I/IntRBTreeNode.md) (test-specific node structure)
  - [RBTreeIterator](../R/RBTreeIterator.md) (tree iteration state structure)
- Called from (representative examples):
  - [test_rb_tree](test_rb_tree.md) (test_rbtree.c:509)

## Notes and Other Information
- Tests the fundamental binary search tree property that in-order traversal yields sorted sequence
- Validates both empty tree handling and populated tree traversal
- Uses strict inequality checks to ensure no duplicate values are encountered during traversal
- Performs completeness validation by counting visited elements and checking boundary values
- Essential component of red-black tree correctness validation
- The test assumes elements 0 through size-1, so lastKey should end at size-1 and count should equal size

## Simplified Source

```c
static void testleftright(int size) {
    RBTree *tree = create_int_rbtree();
    IntRBTreeNode *node;
    RBTreeIterator iter;
    int lastKey = -1;
    int count = 0;

    // Test empty tree - should produce no elements
    rbt_begin_iterate(tree, LeftRightWalk, &iter);
    if (rbt_iterate(&iter) != NULL)
        elog(ERROR, "left-right walk over empty tree produced an element");

    // Populate tree with consecutive numbers 0..size-1
    rbt_populate(tree, size, 1);

    // Traverse tree and verify elements are in increasing order
    rbt_begin_iterate(tree, LeftRightWalk, &iter);
    while ((node = (IntRBTreeNode *) rbt_iterate(&iter)) != NULL) {
        // Ensure strict increasing order
        if (node->key <= lastKey)
            elog(ERROR, "left-right walk gives elements not in sorted order");
        lastKey = node->key;
        count++;
    }

    // Verify completeness: reached end and visited all elements
    if (lastKey != size - 1)
        elog(ERROR, "left-right walk did not reach end");
    if (count != size)
        elog(ERROR, "left-right walk missed some elements");
}
```