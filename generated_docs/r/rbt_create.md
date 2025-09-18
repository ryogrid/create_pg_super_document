# rbt_create

## Location
[src/backend/lib/rbtree.c:102-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/rbtree.c#L102-L126)

## Overview
Creates and initializes a new empty Red-Black Tree data structure with customizable node manipulation functions.

## Definition


## Detailed Description
This function creates a new Red-Black Tree instance by allocating memory for the RBTree structure and initializing it with user-provided manipulation functions. The tree starts empty with its root set to RBTNIL. The function allows for complete customization of how nodes are compared, combined, allocated, and freed, making it suitable for different data types and memory management strategies. The tree itself is allocated using palloc() in the caller's memory context, while actual tree contents are managed by the caller through the provided function pointers.

## Parameters / Member Variables
- : The actual size of tree nodes (must be greater than sizeof(RBTNode))
- : Function pointer to compare two RBTNodes for ordering (less/equal/greater)
- : Function pointer to merge an existing tree entry with a new one
- : Function pointer to allocate new RBTNode instances
- : Function pointer to free RBTNode instances (can be NULL if no cleanup needed)
- : Passthrough pointer that will be passed to all manipulation functions

## Dependencies
- Functions called/Symbols referenced:
  - [RBTree](../R/RBTree.md) (structure type)
  - [RBTNode](../R/RBTNode.md) (structure type)  
  - RBTNIL (constant for empty tree marker)
  - [palloc](../p/palloc.md) (memory allocation function)
  - Assert (assertion macro)
- Called from (representative examples):
  - [ginInitBA](../g/ginInitBA.md) (GIN index bulk allocation initialization)
  - create_int_rbtree (test module function)

## Notes and Other Information
- The combiner's righthand argument will be a "proposed" tree node where RBTNode fields aren't valid
- Either input to the comparator may be a "proposed" node
- The freefunc should typically be pfree or equivalent and should NOT free subsidiary data
- Tree destruction is typically handled by resetting or deleting the memory context
- All tree contents are allocated and managed by the caller, not by the tree implementation itself
- The function validates that node_size is larger than the base RBTNode structure size