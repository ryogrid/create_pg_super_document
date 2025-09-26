# testfindltgt

## Location
src/test/modules/test_rbtree/test_rbtree.c: 287 - 386

## Overview
A comprehensive test function that validates the correctness of the rbt_find_less() and rbt_find_great() functions by searching for keys and iterating through lesser and greater keys in a Red-Black Tree.

## Definition


## Detailed Description
This test function performs thorough validation of Red-Black Tree range search functionality. It creates a tree populated with natural numbers from 1 to size, selects a random key within the range, and then systematically tests both rbt_find_less() and rbt_find_great() functions. The test includes:

1. **Equal Match Validation**: Verifies that both functions correctly find the same node when searching for an existing key with equal_match=true
2. **Lesser Keys Iteration**: Iterates through all keys less than the search key, validating that rbt_find_less() consistently finds the next smaller value
3. **Greater Keys Iteration**: Iterates through all keys greater than the search key, validating that rbt_find_great() consistently finds the next larger value
4. **Random Deletion Testing**: Randomly deletes found nodes during iteration to test the functions' behavior with a dynamically changing tree structure
5. **Boundary Testing**: Tests edge cases with out-of-bounds searches to ensure functions return NULL appropriately

## Parameters / Member Variables
- : The number of natural numbers (1 to size) to insert into the test tree, also used as the upper bound for random key selection

## Dependencies
- Functions called/Symbols referenced:
  - create_int_rbtree: Creates a new integer Red-Black Tree
  - pg_prng_uint64_range: Generates random numbers for key selection and deletion decisions  
  - rbt_populate: Populates tree with natural numbers
  - rbt_find_less: Finds the largest key less than or equal to the search key
  - rbt_find_great: Finds the smallest key greater than or equal to the search key
  - rbt_delete: Removes nodes from the tree
  - elog: Reports test failures with ERROR level
- Called from (representative examples):
  - test_rb_tree: Main test function that orchestrates all Red-Black Tree tests

## Notes and Other Information
- Uses IntRBTreeNode structure for test data with integer keys
- Employs probabilistic testing by randomly deleting nodes during traversal
- Includes comprehensive boundary testing for edge cases
- Part of the PostgreSQL test suite for validating Red-Black Tree implementation
- The function ensures that both search functions behave consistently when finding equal matches
- Tests both inclusive (equal_match=true) and exclusive (equal_match=false) search modes