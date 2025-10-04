# rbt_insert

## Location
[src/backend/lib/rbtree.c:453-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L453-L520)

## Overview
Inserts a new value into the Red-Black Tree, handling both new insertions and merging with existing nodes.

## Definition

```c
RBTNode *
rbt_insert(RBTree *rbt, const RBTNode *data, bool *isNew)
```
## Detailed Description
This function provides the primary insertion interface for Red-Black Trees. It performs a standard binary search tree insertion followed by Red-Black Tree rebalancing. The function handles two scenarios:

1. **New insertion**: If the data represents a value not present in the tree, a new node is created, inserted at the appropriate location, and the tree is rebalanced.

2. **Existing value**: If a node with the same key already exists, the combiner function is called to merge the new data with the existing node.

The insertion process follows these steps:
1. Traverse the tree to find the insertion point using the comparator function
2. If a matching node is found, apply the combiner function and return the existing node
3. If no match is found, allocate a new red node using the tree's allocator function
4. Copy the data into the new node and insert it at the appropriate position
5. Call rbt_insert_fixup to restore Red-Black Tree properties

The data parameter represents the value to insert, but only the extra data beyond the RBTNode fields is of interest - the RBTNode fields are set up by this function.

## Parameters / Member Variables
- `*rbt`: Pointer to the Red-Black Tree structure
- `*data`: Pointer to the data to be inserted (RBTNode fields need not be initialized)
- `*isNew`: Pointer to boolean flag that will be set to true if a new node was created, false if existing node was updated
## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md), RBTNode (tree and node structure types)
  - RBTNIL (sentinel value for null nodes)
  - RBTRED (red color constant)
  - color (node color field)
  - [rbt_copy_data](rbt_copy_data.md) (copies data between nodes)
  - [rbt_insert_fixup](rbt_insert_fixup.md) (rebalances tree after insertion)
- Called from (representative examples):
  - [ginInsertBAEntry](../g/ginInsertBAEntry.md) (in ginbulk.c:166)
  - [rbt_populate](rbt_populate.md) (in test_rbtree.c:138, 150)

## Notes and Other Information
- This is a public API function for Red-Black Tree insertion
- Time complexity is O(log n) where n is the number of nodes in the tree
- The function is safe to call with existing keys due to the combiner mechanism
- New nodes are always initially colored red, with fixup handling any violations
- The data parameter is never modified - it's typically a local variable in the caller
- Returns a pointer to either the newly created node or the existing node that was updated
- Critical for maintaining Red-Black Tree properties while providing efficient insertion
- Uses the tree's configured comparator, combiner, and allocator functions for flexibility

## Simplified Source

```c
RBTNode *rbt_insert(RBTree *rbt, const RBTNode *data, bool *isNew)
{
    RBTNode *current, *parent, *x;
    int cmp = 0;

    // Search for insertion point or existing node
    current = rbt->root;
    parent = NULL;

    while (current != RBTNIL) {
        cmp = rbt->comparator(data, current, rbt->arg);

        if (cmp == 0) {
            // Found existing node - merge data using combiner
            rbt->combiner(current, data, rbt->arg);
            *isNew = false;
            return current;
        }

        parent = current;
        current = (cmp < 0) ? current->left : current->right;
    }

    // Create new node for insertion
    *isNew = true;
    x = rbt->allocfunc(rbt->arg);

    // Initialize new node as red leaf
    x->color = RBTRED;
    x->left = RBTNIL;
    x->right = RBTNIL;
    x->parent = parent;
    rbt_copy_data(rbt, x, data);

    // Insert node at appropriate position
    if (parent) {
        if (cmp < 0)
            parent->left = x;
        else
            parent->right = x;
    } else {
        rbt->root = x;
    }

    // Restore red-black tree properties
    rbt_insert_fixup(rbt, x);

    return x;
}
```