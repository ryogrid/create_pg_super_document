# IntRBTreeNode

## Location
[src/test/modules/test_rbtree/test_rbtree.c:27-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L27-L31)

## Overview
IntRBTreeNode is a test-specific structure used in PostgreSQL's red-black tree test module that extends the base RBTNode to store integer keys for testing red-black tree operations.

## Definition
```c
typedef struct IntRBTreeNode
{
    RBTNode     rbtnode;
    int         key;
} IntRBTreeNode;
```

## Detailed Description
IntRBTreeNode serves as a concrete implementation of a red-black tree node specifically designed for testing purposes in PostgreSQL's test suite. This structure demonstrates the intended usage pattern for RBTNode, where it acts as the first field of a larger struct containing application-specific payload data. In this case, the payload is a simple integer key used to test various red-black tree operations such as insertion, deletion, searching, and traversal.

The structure follows the established pattern for red-black tree nodes in PostgreSQL, where the generic RBTNode provides the tree structure (color, parent, left and right child pointers), while the containing struct adds the domain-specific data. This design allows the red-black tree implementation to remain generic while supporting different types of payloads.

## Parameters / Member Variables
- `rbtnode`: Base RBTNode structure containing red-black tree metadata (color, parent, left/right child pointers)
- `key`: Integer value used as the sorting key and primary data payload for testing tree operations

## Dependencies
- Functions called/Symbols referenced:
  - [RBTNode](../R/RBTNode.md) (base red-black tree node structure)

- Called from (representative examples):
  - [irbt_cmp](../i/irbt_cmp.md) (comparison function for integer keys)
  - [irbt_combine](../i/irbt_combine.md) (node combination function)
  - [irbt_alloc](../i/irbt_alloc.md) (node allocation function)
  - [create_int_rbtree](../c/create_int_rbtree.md) (tree creation function)
  - [rbt_populate](../r/rbt_populate.md) (tree population for testing)
  - [testleftright](../t/testleftright.md) (left-right traversal tests)
  - [testrightleft](../t/testrightleft.md) (right-left traversal tests)
  - [testfind](../t/testfind.md) (node finding tests)
  - [testfindltgt](../t/testfindltgt.md) (less-than/greater-than finding tests)
  - [testleftmost](../t/testleftmost.md) (leftmost node tests)
  - [testdelete](../t/testdelete.md) (node deletion tests)

## Notes and Other Information
- This structure is located in src/test/modules/test_rbtree/test_rbtree.c and is used exclusively for testing red-black tree functionality
- The structure demonstrates proper usage of RBTNode as the first field, which is required for the red-black tree implementation to work correctly
- The integer key serves as both the comparison value and the primary data stored in test scenarios
- Used extensively throughout the test module to validate red-black tree operations including insertion, deletion, searching, and various traversal patterns
- The design showcases how to extend the generic RBTNode for specific use cases while maintaining compatibility with the underlying red-black tree algorithms