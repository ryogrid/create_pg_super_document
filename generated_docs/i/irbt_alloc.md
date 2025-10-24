# irbt_alloc

## Location
[src/test/modules/test_rbtree/test_rbtree.c:64-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L64-L70)

## Overview
A static node allocator function used in PostgreSQL's Red-Black Tree test module to allocate memory for new IntRBTreeNode instances.

## Definition
```c
static RBTNode *irbt_alloc(void *arg)
```

## Detailed Description
This function serves as the memory allocator callback for integer-based Red-Black Tree nodes in PostgreSQL's test infrastructure. It allocates memory for a new IntRBTreeNode structure using PostgreSQL's palloc() memory management function and returns it cast as a generic RBTNode pointer.

The function provides a standardized interface for node allocation that the Red-Black Tree library can use without knowing the specific node type being allocated. This abstraction allows the tree implementation to work with different node types while delegating memory allocation responsibilities to type-specific allocator functions.

## Parameters / Member Variables
- `arg`: Unused argument parameter (required by RBTNode allocator interface, allows passing context if needed)

## Dependencies
- Functions called/Symbols referenced:
  - [RBTNode](../R/RBTNode.md) (generic Red-Black Tree node type)
  - [IntRBTreeNode](../I/IntRBTreeNode.md) (integer-specific node structure)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
- Called from (representative examples):
  - [create_int_rbtree](../c/create_int_rbtree.md) (used as allocator function in tree creation)

## Notes and Other Information
- This is a static function used only within the test_rbtree module
- Uses PostgreSQL's palloc() for memory allocation, which provides automatic cleanup on transaction/context end
- Returns memory cast as generic RBTNode pointer for type abstraction
- Part of PostgreSQL's Red-Black Tree testing framework
- The allocated memory size is specifically sizeof(IntRBTreeNode) but returned as generic RBTNode*
- Follows the standard Red-Black Tree allocator interface pattern

## Simplified Source

```c
static RBTNode *irbt_alloc(void *arg) {
    // Allocate memory for an IntRBTreeNode and return as generic RBTNode
    return (RBTNode *) palloc(sizeof(IntRBTreeNode));
}
```