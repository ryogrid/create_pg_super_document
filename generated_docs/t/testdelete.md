# testdelete

## Location
[src/test/modules/test_rbtree/test_rbtree.c:409-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L409-L502)

## Overview
A comprehensive test function that validates the correctness of the rbt_delete() operation by performing random deletions and verifying the tree maintains its integrity throughout the process.

## Definition

```c
static void
testdelete(int size, int delsize)
```
## Detailed Description
This test function performs thorough validation of Red-Black Tree deletion functionality through a multi-phase testing approach:

1. **Tree Population**: Creates a tree populated with consecutive natural numbers from 0 to size-1
2. **Random Selection**: Randomly selects delsize unique elements to delete, ensuring no duplicates through a boolean tracking array
3. **Deletion Phase**: Systematically deletes the selected elements, verifying each element exists before deletion
4. **Verification Phase**: Validates that deleted elements are no longer findable and non-deleted elements remain intact
5. **Complete Cleanup**: Deletes all remaining elements to test tree reduction to empty state
6. **Final Validation**: Confirms the tree is completely empty using rbt_leftmost()

The test uses random selection to provide comprehensive coverage of different deletion scenarios and tree configurations, ensuring the Red-Black Tree maintains its balanced properties throughout the deletion process.

## Parameters / Member Variables
- `size`: The total number of natural numbers (0 to size-1) to initially insert into the test tree
- `delsize`: The number of elements to randomly select and delete during the first deletion phase
## Dependencies
- Functions called/Symbols referenced:
  - [create_int_rbtree](../c/create_int_rbtree.md): Creates a new integer Red-Black Tree
  - [rbt_populate](../r/rbt_populate.md): Populates tree with consecutive natural numbers  
  - [palloc](../p/palloc.md): Allocates memory for deleteIds array
  - [palloc0](../p/palloc0.md): Allocates zero-initialized memory for chosen boolean array
  - [pg_prng_uint64_range](../p/pg_prng_uint64_range.md): Generates random numbers for element selection
  - [rbt_find](../r/rbt_find.md): Locates nodes in the tree before deletion
  - [rbt_delete](../r/rbt_delete.md): Removes nodes from the tree
  - [rbt_leftmost](../r/rbt_leftmost.md): Verifies tree is empty after complete deletion
  - [pfree](../p/pfree.md): Frees allocated memory arrays
  - elog: Reports test failures with ERROR level
- Called from (representative examples):
  - [test_rb_tree](test_rb_tree.md): Main test function that orchestrates all Red-Black Tree tests

## Notes and Other Information
- Uses IntRBTreeNode structure for test data with integer keys
- Employs a two-phase deletion strategy: partial random deletion followed by complete cleanup
- Uses collision resolution with modular arithmetic when randomly selecting elements
- Validates both positive (element should exist) and negative (element should not exist) cases
- Part of the PostgreSQL test suite for validating Red-Black Tree implementation
- Memory management includes proper cleanup of allocated arrays
- Tests the tree's ability to be completely emptied and return to initial state

## Simplified Source

```c
static void testdelete(int size, int delsize) {
    RBTree *tree = create_int_rbtree();
    int *deleteIds;
    bool *chosen;

    // Populate tree with consecutive natural numbers 0 to size-1
    rbt_populate(tree, size, 1);

    // Choose unique random IDs to delete
    deleteIds = (int *) palloc(delsize * sizeof(int));
    chosen = (bool *) palloc0(size * sizeof(bool));

    for (int i = 0; i < delsize; i++) {
        int k = pg_prng_uint64_range(&pg_global_prng_state, 0, size - 1);

        // Find next unchosen element using wrap-around
        while (chosen[k])
            k = (k + 1) % size;

        deleteIds[i] = k;
        chosen[k] = true;
    }

    // Delete the randomly selected elements
    for (int i = 0; i < delsize; i++) {
        IntRBTreeNode find = {.key = deleteIds[i]};
        IntRBTreeNode *node = (IntRBTreeNode *) rbt_find(tree, &find);

        if (!node || node->key != deleteIds[i])
            elog(ERROR, "Expected element not found during deletion");

        rbt_delete(tree, (RBTNode *) node);
    }

    // Verify deletion results - deleted elements absent, others present
    for (int i = 0; i < size; i++) {
        IntRBTreeNode searchNode = {.key = i};
        IntRBTreeNode *result = (IntRBTreeNode *) rbt_find(tree, &searchNode);

        if (chosen[i]) {
            // Deleted element should be absent
            if (result != NULL)
                elog(ERROR, "Deleted element still present");
        } else {
            // Non-deleted element should be present
            if (!result || result->key != i)
                elog(ERROR, "Delete operation removed wrong value");
        }
    }

    // Delete all remaining elements to test empty tree reduction
    for (int i = 0; i < size; i++) {
        if (chosen[i]) continue;  // Already deleted

        IntRBTreeNode find = {.key = i};
        IntRBTreeNode *node = (IntRBTreeNode *) rbt_find(tree, &find);

        if (!node || node->key != i)
            elog(ERROR, "Expected element not found during cleanup");

        rbt_delete(tree, (RBTNode *) node);
    }

    // Verify tree is completely empty
    if (rbt_leftmost(tree) != NULL)
        elog(ERROR, "Failed to delete all elements");

    pfree(deleteIds);
    pfree(chosen);
}
```