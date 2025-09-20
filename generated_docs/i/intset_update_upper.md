# intset_update_upper

## Location
[src/backend/lib/integerset.c:481-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L481-L553)

## Overview
A recursive static function that manages the insertion of downlinks into parent nodes of the IntegerSet B-tree, handling node splits and tree growth as necessary.

## Definition

```c
static void
intset_update_upper(IntegerSet *intset, int level, intset_node *child,
					uint64 child_key)
```
## Detailed Description
The  function is a critical component of the IntegerSet B-tree maintenance system. It handles the upward propagation of changes when new nodes are created at lower levels of the tree. This function ensures the B-tree remains balanced and maintains its structural integrity.

Key responsibilities include:
1. **Root expansion**: When the tree needs to grow in height, it creates a new root node and properly establishes the tree hierarchy
2. **Parent node management**: Inserts downlinks (pointers to child nodes) into appropriate parent nodes
3. **Node splitting**: When parent nodes become full (exceed MAX_INTERNAL_ITEMS), it creates new internal nodes and recursively propagates the change upward
4. **Tree level tracking**: Maintains the rightmost_nodes array to track the rightmost node at each level for efficient insertion

The function uses a recursive approach to handle cascading splits that may occur when nodes at multiple levels become full simultaneously.

## Parameters
- : Pointer to the IntegerSet structure being modified
- : The tree level where the parent node resides (0 = leaf level)
- : Pointer to the child node that needs a downlink in its parent
- : The key value associated with the child node (first value in the child)

## Dependencies
- Functions called/Symbols referenced:
  - [intset_new_internal_node](intset_new_internal_node.md)
  - [intset_update_upper](intset_update_upper.md) (recursive call)
  - [intset_internal_node](intset_internal_node.md)
  - [intset_leaf_node](intset_leaf_node.md)
  - [intset_node](intset_node.md)
  - MAX_TREE_LEVELS
  - MAX_INTERNAL_ITEMS
- Called from (representative examples):
  - [intset_flush_buffered_values](intset_flush_buffered_values.md)
  - [intset_update_upper](intset_update_upper.md) (recursive)
  - [IntegerSet](../I/IntegerSet.md) (during finalization)

## Notes and Other Information
- This is a static function, only accessible within the integerset.c file
- Implements recursive tree balancing to maintain B-tree properties
- Handles the creation of new root nodes when the tree grows in height
- Enforces maximum tree depth (MAX_TREE_LEVELS) to prevent infinite growth
- Critical for maintaining the performance characteristics of the B-tree structure
- Updates the rightmost_nodes tracking array to optimize future insertions
- The child_key parameter represents the minimum value that can be found in the child subtree