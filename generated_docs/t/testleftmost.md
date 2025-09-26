# testleftmost

## Location
src/test/modules/test_rbtree/test_rbtree.c: 387 - 408

## Overview
A test function that validates the correctness of the rbt_leftmost() operation by verifying it always returns the smallest element in the Red-Black Tree.

## Definition


## Detailed Description
This test function performs validation of the Red-Black Tree leftmost node retrieval functionality. It tests two key scenarios:

1. **Empty Tree Testing**: Verifies that rbt_leftmost() returns NULL when called on an empty tree
2. **Populated Tree Testing**: Creates a tree populated with consecutive natural numbers from 0 to size-1, then validates that rbt_leftmost() correctly identifies and returns the node with key value 0 (the smallest element)

The test ensures that the leftmost operation correctly traverses to the leftmost node in the tree structure, which by the properties of a binary search tree should contain the minimum value.

## Parameters / Member Variables
- : The number of natural numbers (0 to size-1) to insert into the test tree for validation

## Dependencies
- Functions called/Symbols referenced:
  - create_int_rbtree: Creates a new integer Red-Black Tree
  - rbt_leftmost: Returns the leftmost (minimum) node in the tree
  - rbt_populate: Populates tree with consecutive natural numbers
  - elog: Reports test failures with ERROR level
- Called from (representative examples):
  - test_rb_tree: Main test function that orchestrates all Red-Black Tree tests

## Notes and Other Information
- Uses IntRBTreeNode structure for test data with integer keys
- Part of the PostgreSQL test suite for validating Red-Black Tree implementation
- The test assumes rbt_populate starts from key 0 when called with parameters (size, 1)
- Simple but essential test ensuring the basic tree traversal property is maintained
- Validates both edge case (empty tree) and normal operation (populated tree)