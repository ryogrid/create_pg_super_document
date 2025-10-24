# irbt_free

## Location
[src/test/modules/test_rbtree/test_rbtree.c:71-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L71-L79)

## Overview
A static node deallocation function used in PostgreSQL's Red-Black Tree test module to free memory for IntRBTreeNode instances.

## Definition
```c
static void irbt_free(RBTNode *node, void *arg)
```

## Detailed Description
This function serves as the memory deallocation callback for integer-based Red-Black Tree nodes in PostgreSQL's test infrastructure. It provides a simple wrapper around PostgreSQL's pfree() function to release memory allocated for tree nodes.

The function accepts a generic RBTNode pointer and deallocates the memory using pfree(), which is PostgreSQL's standard memory deallocation function that pairs with palloc(). This provides a standardized interface for node deallocation that the Red-Black Tree library can use without knowing the specific node type being freed.

## Parameters / Member Variables
- `node`: Pointer to the RBTNode to be deallocated (originally allocated as IntRBTreeNode)
- `arg`: Unused argument parameter (required by RBTNode deallocator interface, allows passing context if needed)

## Dependencies
- Functions called/Symbols referenced:
  - [RBTNode](../R/RBTNode.md) (generic Red-Black Tree node type)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [RBTree](../R/RBTree.md) (referenced in broader context)
- Called from (representative examples):
  - [create_int_rbtree](../c/create_int_rbtree.md) (used as deallocator function in tree creation)

## Notes and Other Information
- This is a static function used only within the test_rbtree module
- Uses PostgreSQL's pfree() for memory deallocation, which pairs with palloc()
- Accepts generic RBTNode pointer for type abstraction, though originally allocated as IntRBTreeNode
- Part of PostgreSQL's Red-Black Tree testing framework
- Follows the standard Red-Black Tree deallocator interface pattern
- Simple wrapper function that provides the required interface without additional complexity

## Simplified Source

```c
static void irbt_free(RBTNode *node, void *arg) {
    // Free the node memory using PostgreSQL's memory manager
    pfree(node);
}
```