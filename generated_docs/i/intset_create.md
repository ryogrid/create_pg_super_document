# intset_create

## Location
[src/backend/lib/integerset.c:284-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L284-L315)

## Overview
Creates a new, initially empty IntegerSet data structure for storing compressed 64-bit integers in PostgreSQL.

## Definition

```c
IntegerSet *
intset_create(void)
```
## Detailed Description
The  function allocates and initializes a new IntegerSet structure in the current memory context. The function sets up an empty B-tree-like structure with all fields initialized to their default values. All subsequent memory allocations for this IntegerSet will be performed in the same memory context that was current when this function was called, regardless of which memory context is active when new integers are added to the set.

The function initializes the IntegerSet with:
- Zero entries and no highest value recorded
- Empty B-tree structure (no levels, no root node)
- No buffered values awaiting insertion
- Inactive iterator state

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - : Memory allocation function
  - : Global variable for current memory context
  - : Function to get allocated memory size
  - : Standard C library function for memory initialization
- Called from (representative examples):
  - : Used in GiST index vacuum operations
  - Various test functions in test_integerset module

## Notes and Other Information
- The IntegerSet is created in the current memory context and will continue to use that context for all future allocations
- The structure is initialized as completely empty with no buffered values or tree nodes
- Memory usage tracking begins immediately with the initial allocation size
- Iterator state is initialized as inactive and ready for use

## Simplified Source

```c
IntegerSet *intset_create(void) {
    IntegerSet *intset;

    // Allocate in current memory context
    intset = (IntegerSet *) palloc(sizeof(IntegerSet));
    intset->context = CurrentMemoryContext;
    intset->mem_used = GetMemoryChunkSpace(intset);

    // Initialize as empty set
    intset->num_entries = 0;
    intset->highest_value = 0;

    // Initialize empty B-tree structure
    intset->num_levels = 0;
    intset->root = NULL;
    memset(intset->rightmost_nodes, 0, sizeof(intset->rightmost_nodes));
    intset->leftmost_leaf = NULL;

    // Initialize empty buffer
    intset->num_buffered_values = 0;

    // Initialize inactive iterator
    intset->iter_active = false;
    intset->iter_node = NULL;
    intset->iter_itemno = 0;
    intset->iter_valueno = 0;
    intset->iter_num_values = 0;
    intset->iter_values = NULL;

    return intset;
}
```