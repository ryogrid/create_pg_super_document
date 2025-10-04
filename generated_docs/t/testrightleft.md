# testrightleft

## Location
[src/test/modules/test_rbtree/test_rbtree.c:204-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L204-L242)

## Overview
Validates the correctness of right-to-left (reverse in-order) tree traversal by ensuring all elements are visited in strictly decreasing order.

## Definition
```c
static void testrightleft(int size)
```

## Detailed Description
testrightleft performs comprehensive validation of the red-black tree's right-to-left traversal functionality. The function tests two critical scenarios:

1. **Empty Tree Validation**: Verifies that iterating over an empty tree in reverse order produces no elements, ensuring the reverse iterator handles edge cases correctly.

2. **Reverse Ordered Traversal Validation**: After populating the tree with consecutive natural numbers (0, 1, 2, ..., size-1) inserted in random order, the function validates that right-to-left traversal visits all elements in strictly decreasing order.

This test complements testleftright by validating the reverse traversal direction. Since red-black trees are binary search trees, reverse in-order traversal should yield elements in descending order. The function ensures both ordering correctness and completeness, verifying that the traversal starts from the largest element (size-1) and ends at the smallest (0).

## Parameters / Member Variables  
- `size`: The number of elements to insert and traverse (creates sequence 0 through size-1, visited in reverse)

## Dependencies
- Functions called/Symbols referenced:
  - [create_int_rbtree](../c/create_int_rbtree.md) (creates test red-black tree)
  - [rbt_begin_iterate](../r/rbt_begin_iterate.md) (initializes tree iterator for RightLeftWalk)
  - [rbt_iterate](../r/rbt_iterate.md) (advances iterator and returns next node)
  - [rbt_populate](../r/rbt_populate.md) (populates tree with test data)
  - elog (PostgreSQL error logging)
  - RightLeftWalk (reverse traversal direction constant)
  - [IntRBTreeNode](../I/IntRBTreeNode.md) (test-specific node structure)
  - [RBTreeIterator](../R/RBTreeIterator.md) (tree iteration state structure)
- Called from (representative examples):
  - [test_rb_tree](test_rb_tree.md) (test_rbtree.c:510)

## Notes and Other Information
- Tests the complementary traversal direction to testleftright, ensuring bidirectional iteration works correctly
- Validates the binary search tree property in reverse: reverse in-order traversal yields descending sequence
- Uses strict inequality checks (>=) to ensure no duplicate values are encountered during reverse traversal  
- Initializes lastKey to size (one beyond maximum value) to properly validate decreasing order
- Performs completeness validation by ensuring final key is 0 and count equals size
- Essential for comprehensive red-black tree traversal validation alongside testleftright

## Simplified Source

```c
static void testrightleft(int size) {
    RBTree *tree = create_int_rbtree();
    IntRBTreeNode *node;
    RBTreeIterator iter;
    int lastKey = size;  // Start above max value for decreasing validation
    int count = 0;

    // Test empty tree - should produce no elements
    rbt_begin_iterate(tree, RightLeftWalk, &iter);
    if (rbt_iterate(&iter) != NULL)
        elog(ERROR, "right-left walk over empty tree produced an element");

    // Populate tree with consecutive numbers 0..size-1
    rbt_populate(tree, size, 1);

    // Traverse tree and verify elements are in decreasing order
    rbt_begin_iterate(tree, RightLeftWalk, &iter);
    while ((node = (IntRBTreeNode *) rbt_iterate(&iter)) != NULL) {
        // Ensure strict decreasing order
        if (node->key >= lastKey)
            elog(ERROR, "right-left walk gives elements not in sorted order");
        lastKey = node->key;
        count++;
    }

    // Verify completeness: reached beginning and visited all elements
    if (lastKey != 0)
        elog(ERROR, "right-left walk did not reach end");
    if (count != size)
        elog(ERROR, "right-left walk missed some elements");
}
```