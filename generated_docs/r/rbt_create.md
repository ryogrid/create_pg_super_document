# rbt_create

## Location
[src/backend/lib/rbtree.c:102-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L102-L126)

## Overview
Creates and initializes a new empty Red-Black Tree data structure with customizable node manipulation functions.

## Definition

```c
RBTree *
rbt_create(Size node_size,
		   rbt_comparator comparator,
		   rbt_combiner combiner,
		   rbt_allocfunc allocfunc,
		   rbt_freefunc freefunc,
		   void *arg)
```
## Detailed Description
This function creates a new Red-Black Tree instance by allocating memory for the RBTree structure and initializing it with user-provided manipulation functions. The tree starts empty with its root set to RBTNIL. The function allows for complete customization of how nodes are compared, combined, allocated, and freed, making it suitable for different data types and memory management strategies. The tree itself is allocated using palloc() in the caller's memory context, while actual tree contents are managed by the caller through the provided function pointers.

## Parameters / Member Variables
- `node_size`: The actual size of tree nodes (must be greater than sizeof(RBTNode))
- `comparator`: Function pointer to compare two RBTNodes for ordering (less/equal/greater)
- `combiner`: Function pointer to merge an existing tree entry with a new one
- `allocfunc`: Function pointer to allocate new RBTNode instances
- `freefunc`: Function pointer to free RBTNode instances (can be NULL if no cleanup needed)
- `*arg`: Passthrough pointer that will be passed to all manipulation functions
## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (structure type)
  - [RBTNode](../R/RBTNode.md) (structure type)  
  - RBTNIL (constant for empty tree marker)
  - [palloc](../p/palloc.md) (memory allocation function)
  - Assert (assertion macro)
- Called from (representative examples):
  - [ginInitBA](../g/ginInitBA.md) (GIN index bulk allocation initialization)
  - [create_int_rbtree](../c/create_int_rbtree.md) (test module function)

## Notes and Other Information
- The combiner's righthand argument will be a "proposed" tree node where RBTNode fields aren't valid
- Either input to the comparator may be a "proposed" node
- The freefunc should typically be pfree or equivalent and should NOT free subsidiary data
- Tree destruction is typically handled by resetting or deleting the memory context
- All tree contents are allocated and managed by the caller, not by the tree implementation itself
- The function validates that node_size is larger than the base RBTNode structure size

## Simplified Source

```c
RBTree *rbt_create(Size node_size,
                   rbt_comparator comparator,
                   rbt_combiner combiner,
                   rbt_allocfunc allocfunc,
                   rbt_freefunc freefunc,
                   void *arg)
{
    // Allocate memory for the tree structure
    RBTree *tree = (RBTree *) palloc(sizeof(RBTree));

    // Validate node size is larger than base structure
    Assert(node_size > sizeof(RBTNode));

    // Initialize tree with empty state
    tree->root = RBTNIL;
    tree->node_size = node_size;

    // Set up function pointers for node operations
    tree->comparator = comparator;
    tree->combiner = combiner;
    tree->allocfunc = allocfunc;
    tree->freefunc = freefunc;

    // Store user context data
    tree->arg = arg;

    return tree;
}
```