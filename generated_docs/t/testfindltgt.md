# testfindltgt

## Location
[src/test/modules/test_rbtree/test_rbtree.c:287-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L287-L386)

## Overview
A comprehensive test function that validates the correctness of the rbt_find_less() and rbt_find_great() functions by searching for keys and iterating through lesser and greater keys in a Red-Black Tree.

## Definition

```c
static void
testfindltgt(int size)
```
## Detailed Description
This test function performs thorough validation of Red-Black Tree range search functionality. It creates a tree populated with natural numbers from 1 to size, selects a random key within the range, and then systematically tests both rbt_find_less() and rbt_find_great() functions. The test includes:

1. **Equal Match Validation**: Verifies that both functions correctly find the same node when searching for an existing key with equal_match=true
2. **Lesser Keys Iteration**: Iterates through all keys less than the search key, validating that rbt_find_less() consistently finds the next smaller value
3. **Greater Keys Iteration**: Iterates through all keys greater than the search key, validating that rbt_find_great() consistently finds the next larger value
4. **Random Deletion Testing**: Randomly deletes found nodes during iteration to test the functions' behavior with a dynamically changing tree structure
5. **Boundary Testing**: Tests edge cases with out-of-bounds searches to ensure functions return NULL appropriately

## Parameters / Member Variables
- `size`: The number of natural numbers (1 to size) to insert into the test tree, also used as the upper bound for random key selection
## Dependencies
- Functions called/Symbols referenced:
  - [create_int_rbtree](../c/create_int_rbtree.md): Creates a new integer Red-Black Tree
  - [pg_prng_uint64_range](../p/pg_prng_uint64_range.md): Generates random numbers for key selection and deletion decisions  
  - [rbt_populate](../r/rbt_populate.md): Populates tree with natural numbers
  - [rbt_find_less](../r/rbt_find_less.md): Finds the largest key less than or equal to the search key
  - [rbt_find_great](../r/rbt_find_great.md): Finds the smallest key greater than or equal to the search key
  - [rbt_delete](../r/rbt_delete.md): Removes nodes from the tree
  - elog: Reports test failures with ERROR level
- Called from (representative examples):
  - [test_rb_tree](test_rb_tree.md): Main test function that orchestrates all Red-Black Tree tests

## Notes and Other Information
- Uses IntRBTreeNode structure for test data with integer keys
- Employs probabilistic testing by randomly deleting nodes during traversal
- Includes comprehensive boundary testing for edge cases
- Part of the PostgreSQL test suite for validating Red-Black Tree implementation
- The function ensures that both search functions behave consistently when finding equal matches
- Tests both inclusive (equal_match=true) and exclusive (equal_match=false) search modes

## Simplified Source

```c
static void testfindltgt(int size) {
    RBTree *tree = create_int_rbtree();

    // Choose random key in range [0, size-1] to ensure we can find greater matches
    int randomKey = pg_prng_uint64_range(&pg_global_prng_state, 0, size - 1);
    IntRBTreeNode searchNode = {.key = randomKey};

    // Populate tree with natural numbers 1 to size
    rbt_populate(tree, size, 1);

    // Test equal match finding - both functions should find same node
    IntRBTreeNode *lteNode = (IntRBTreeNode *) rbt_find_less(tree, &searchNode, true);
    IntRBTreeNode *gteNode = (IntRBTreeNode *) rbt_find_great(tree, &searchNode, true);

    if (!lteNode || lteNode->key != randomKey)
        elog(ERROR, "rbt_find_less() didn't find the equal key");
    if (!gteNode || gteNode->key != randomKey)
        elog(ERROR, "rbt_find_great() didn't find the equal key");
    if (lteNode != gteNode)
        elog(ERROR, "Functions found different equal keys");

    // Test finding lesser keys with random deletions
    bool keyDeleted = false;
    for (searchNode.key = randomKey; searchNode.key > 0; searchNode.key--) {
        IntRBTreeNode *node = (IntRBTreeNode *) rbt_find_less(tree, &searchNode, keyDeleted);

        if (!node || node->key >= searchNode.key)
            elog(ERROR, "rbt_find_less() didn't find a lesser key");

        // Randomly delete found node
        keyDeleted = (pg_prng_uint64_range(&pg_global_prng_state, 0, 1) == 1);
        if (keyDeleted)
            rbt_delete(tree, (RBTNode *) node);
    }

    // Test finding greater keys with random deletions
    keyDeleted = false;
    for (searchNode.key = randomKey; searchNode.key < size - 1; searchNode.key++) {
        IntRBTreeNode *node = (IntRBTreeNode *) rbt_find_great(tree, &searchNode, keyDeleted);

        if (!node || node->key <= searchNode.key)
            elog(ERROR, "rbt_find_great() didn't find a greater key");

        // Randomly delete found node
        keyDeleted = (pg_prng_uint64_range(&pg_global_prng_state, 0, 1) == 1);
        if (keyDeleted)
            rbt_delete(tree, (RBTNode *) node);
    }

    // Test boundary conditions - should find nothing
    searchNode.key = -1;
    if (rbt_find_less(tree, &searchNode, true) != NULL)
        elog(ERROR, "Found element below tree range");

    searchNode.key = size;
    if (rbt_find_great(tree, &searchNode, true) != NULL)
        elog(ERROR, "Found element above tree range");
}
```