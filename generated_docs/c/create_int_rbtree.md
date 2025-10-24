# create_int_rbtree

## Location
[src/test/modules/test_rbtree/test_rbtree.c:80-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L80-L93)

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
  - [rbt_create](../r/rbt_create.md) (core Red-Black Tree creation function)
  - [IntRBTreeNode](../I/IntRBTreeNode.md) (integer-specific node structure type)
  - [irbt_cmp](../i/irbt_cmp.md) (node comparator function)
  - [irbt_combine](../i/irbt_combine.md) (node combiner function)
  - [irbt_alloc](../i/irbt_alloc.md) (node allocator function)
  - [irbt_free](../i/irbt_free.md) (node deallocator function)
- Called from (representative examples):
  - [testleftright](../t/testleftright.md) (test function for left-right rotation scenarios)
  - [testrightleft](../t/testrightleft.md) (test function for right-left rotation scenarios)
  - [testfind](../t/testfind.md) (test function for node finding operations)
  - [testfindltgt](../t/testfindltgt.md) (test function for less-than/greater-than finding)
  - [testleftmost](../t/testleftmost.md) (test function for leftmost node operations)
  - [testdelete](../t/testdelete.md) (test function for node deletion operations)

## Notes and Other Information
- This is a static function used only within the test_rbtree module
- Provides a standardized way to create integer-based Red-Black Trees for testing
- Passes NULL as the final argument to rbt_create(), indicating no additional context needed
- Part of PostgreSQL's Red-Black Tree testing framework
- Encapsulates all the callback function setup required for integer tree operations
- Used extensively by various test functions to create consistent tree configurations
- Returns a fully initialized RBTree ready for integer key operations

## Simplified Source

```c
static RBTree *create_int_rbtree(void) {
    // Create Red-Black Tree configured for integer nodes
    return rbt_create(sizeof(IntRBTreeNode),
                      irbt_cmp,      // Comparison function
                      irbt_combine,  // Combination function
                      irbt_alloc,    // Allocation function
                      irbt_free,     // Deallocation function
                      NULL);         // No additional context
}
```