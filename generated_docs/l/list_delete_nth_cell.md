# list_delete_nth_cell

## Location
src/backend/nodes/list.c: 767 - 840

## Overview
Deletes the n-th cell (zero-indexed) from a PostgreSQL List, handling memory management and list structure updates appropriately.

## Definition
```c
List *list_delete_nth_cell(List *list, int n)
```

## Detailed Description
The `list_delete_nth_cell` function removes a specific cell from a PostgreSQL List structure based on its position (zero-indexed). The function implements efficient memory management by either collapsing the removed element in-place or allocating new memory depending on debugging configuration. When the last element is removed, the entire list is freed and NIL is returned. The function maintains list invariants and handles different memory management strategies based on whether `DEBUG_LIST_MEMORY_USAGE` is enabled.

The deletion operation has O(n) time complexity proportional to the distance from the deleted element to the end of the list, as subsequent elements must be shifted. The function includes comprehensive memory debugging support with options to wipe freed memory or mark it as inaccessible using Valgrind annotations.

## Parameters / Member Variables
- `list`: Pointer to the List structure from which to delete the cell. The list is modified in-place or replaced.
- `n`: Zero-based index of the cell to delete. Must be >= 0 and < list->length.

## Dependencies
- Functions called/Symbols referenced:
  - [check_list_invariants](../c/check_list_invariants.md) - Validates list structure integrity (called at start and optionally at end)
  - [list_free](list_free.md) - Frees the entire list when deleting the last element
  - memmove - Moves remaining elements when not in debug mode
  - GetMemoryChunkContext - Gets the memory context of the list for new allocations
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) - Allocates new memory for list elements in debug mode
  - memcpy - Copies list elements in debug mode
  - [pfree](../p/pfree.md) - Frees old element array in debug mode
  - [wipe_mem](../w/wipe_mem.md) - Clears freed memory when CLOBBER_FREED_MEMORY is enabled
  - VALGRIND_MAKE_MEM_NOACCESS - Marks freed memory as inaccessible for debugging

- Called from (representative examples):
  - [MergeAttributes](../M/MergeAttributes.md) - Used in table command processing
  - [list_delete_cell](list_delete_cell.md) - Higher-level function for deleting cells by reference
  - list_delete_first - Convenience function for deleting the first element
  - [process_equivalence](../p/process_equivalence.md) - Used in query optimization equivalence class processing
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md) - Used in join path optimization
  - foreach_delete_current - Macro for safe deletion during iteration

## Notes and Other Information
- Returns the modified list or NIL if the list becomes empty
- Asserts that the index n is valid (0 <= n < list->length)
- When deleting the last element, the entire list structure is freed and NIL is returned
- Time complexity is O(k) where k is the number of elements after position n
- In non-debug mode, uses efficient memmove to shift remaining elements
- In debug mode (`DEBUG_LIST_MEMORY_USAGE`), allocates completely new memory to help detect use-after-free bugs
- Handles both initial_elements (stack-allocated) and dynamically allocated element arrays
- Includes extensive memory debugging features with Valgrind support and memory wiping options
- Part of PostgreSQL's generic List API located in src/backend/nodes/list.c
- Essential for dynamic list manipulation throughout the PostgreSQL system