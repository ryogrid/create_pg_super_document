# create_int_rbtree

## Location
src/test/modules/test_rbtree/test_rbtree.c: 80 - 93

## Overview
A static factory function that creates and initializes a Red-Black Tree specifically designed for storing integer keys using specialized callback functions.

## Definition
```c
static RBTree *create_int_rbtree(void)
```

## Detailed Description
This function serves as a factory method for creating integer-based Red-Black Trees in PostgreSQL's test infrastructure. It encapsulates the setup of a Red-Black Tree by calling rbt_create() with all the necessary callback functions and configuration parameters specific to integer node handling.

The function creates a tree configured with specialized functions for comparing, combining, allocating, and freeing IntRBTreeNode structures. It provides a clean abstraction that hides the complexity of Red-Black Tree initialization from test code, making it easy to create properly configured integer trees for testing purposes.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - rbt_create (core Red-Black Tree creation function)
  - IntRBTreeNode (integer-specific node structure type)
  - irbt_cmp (node comparator function)
  - irbt_combine (node combiner function)
  - irbt_alloc (node allocator function)
  - irbt_free (node deallocator function)
- Called from (representative examples):
  - testleftright (test function for left-right rotation scenarios)
  - testrightleft (test function for right-left rotation scenarios)
  - testfind (test function for node finding operations)
  - testfindltgt (test function for less-than/greater-than finding)
  - testleftmost (test function for leftmost node operations)
  - testdelete (test function for node deletion operations)

## Notes and Other Information
- This is a static function used only within the test_rbtree module
- Provides a standardized way to create integer-based Red-Black Trees for testing
- Passes NULL as the final argument to rbt_create(), indicating no additional context needed
- Part of PostgreSQL's Red-Black Tree testing framework
- Encapsulates all the callback function setup required for integer tree operations
- Used extensively by various test functions to create consistent tree configurations
- Returns a fully initialized RBTree ready for integer key operations