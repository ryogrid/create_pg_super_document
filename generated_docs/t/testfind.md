# testfind

## Location
[src/test/modules/test_rbtree/test_rbtree.c:243-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L243-L286)

## Overview
Validates the correctness of the rbt_find operation by testing both successful searches for inserted elements and failed searches for non-existent elements.

## Definition
```c
static void testfind(int size)
```

## Detailed Description
testfind performs comprehensive validation of the red-black tree's search functionality through a two-phase testing approach:

1. **Positive Search Validation**: The function populates the tree with even integers (0, 2, 4, 6, ..., 2*(size-1)) using a step size of 2. It then systematically searches for each inserted element to ensure that rbt_find correctly locates all existing nodes and returns the exact matching node with the correct key value.

2. **Negative Search Validation**: The function searches for odd integers (-1, 1, 3, 5, ..., 2*size+1) which were deliberately not inserted into the tree. This validates that rbt_find correctly returns NULL for non-existent elements, including edge cases like values before the minimum (-1) and after the maximum (2*size+1).

This dual approach ensures both the precision of successful searches and the correctness of failed searches, which are equally important for tree integrity and application reliability.

## Parameters / Member Variables
- `size`: The number of even integers to insert (creates sequence 0, 2, 4, ..., 2*(size-1))

## Dependencies
- Functions called/Symbols referenced:
  - [create_int_rbtree](../c/create_int_rbtree.md) (creates test red-black tree)
  - [rbt_populate](../r/rbt_populate.md) (populates tree with even integers using step=2)
  - [rbt_find](../r/rbt_find.md) (searches for elements in the tree)
  - elog (PostgreSQL error logging)
  - [IntRBTreeNode](../I/IntRBTreeNode.md) (test-specific node structure)
  - [RBTNode](../R/RBTNode.md) (generic red-black tree node type)
- Called from (representative examples):
  - [test_rb_tree](test_rb_tree.md) (test_rbtree.c:511)

## Notes and Other Information
- Uses even integers to create gaps in the data, enabling systematic testing of non-existent elements (odd integers)
- Validates both the existence check (NULL vs non-NULL) and value correctness (matching key) for successful searches
- Tests edge cases by searching for values outside the inserted range (-1 and 2*size+1)
- The step=2 parameter to rbt_populate creates the even integer sequence 0, 2, 4, ..., 2*(size-1)
- Critical for validating the fundamental search operation that underlies most tree-based data structure usage
- Ensures that search failures are correctly reported rather than returning incorrect matches

## Simplified Source

```c
static void testfind(int size) {
    RBTree *tree = create_int_rbtree();
    int i;

    // Insert even integers: 0, 2, 4, 6, ..., 2*(size-1)
    rbt_populate(tree, size, 2);

    // Test: all inserted elements should be found
    for (i = 0; i < size; i++) {
        IntRBTreeNode node;
        IntRBTreeNode *resultNode;

        node.key = 2 * i;  // Search for even numbers
        resultNode = (IntRBTreeNode *) rbt_find(tree, (RBTNode *) &node);

        if (resultNode == NULL)
            elog(ERROR, "inserted element was not found");
        if (node.key != resultNode->key)
            elog(ERROR, "find operation in rbtree gave wrong result");
    }

    // Test: non-inserted elements (odd numbers) should not be found
    // Test range: -1, 1, 3, 5, ..., 2*size+1
    for (i = -1; i <= 2 * size; i += 2) {
        IntRBTreeNode node;
        IntRBTreeNode *resultNode;

        node.key = i;  // Search for odd numbers
        resultNode = (IntRBTreeNode *) rbt_find(tree, (RBTNode *) &node);

        if (resultNode != NULL)
            elog(ERROR, "not-inserted element was found");
    }
}
```