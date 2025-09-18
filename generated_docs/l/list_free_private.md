# list_free_private

## Location
[src/backend/nodes/list.c:1520-1545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1520-L1545)

## Overview
Internal helper function that frees all memory associated with a list, with an option to also free the pointed-to elements.

## Definition


## Detailed Description
This is a private static function that serves as the common implementation for both `list_free()` and `list_free_deep()`. It handles the memory deallocation of a PostgreSQL List structure and optionally the elements it contains. The function first checks if the list is NIL (null) and returns early if so. It then validates the list structure using invariant checks.

When the `deep` parameter is true, the function iterates through all list elements and frees each one using `pfree()`. After optionally freeing the elements, it checks if the elements array was dynamically allocated (different from the initial_elements inline array) and frees it if necessary. Finally, it frees the list structure itself.

This function provides a unified memory management approach for PostgreSQL's list implementation, handling both shallow and deep freeing scenarios.

## Parameters / Member Variables
- `list`: The List structure to free. Can be NIL, in which case the function returns immediately.
- `deep`: Boolean flag indicating whether to free the pointed-to elements in addition to the list structure itself.

## Dependencies
- Functions called/Symbols referenced:
  - [check_list_invariants](../c/check_list_invariants.md)
  - [pfree](../p/pfree.md)
  - lfirst
- Called from (representative examples):
  - [list_free](list_free.md)
  - [list_free_deep](list_free_deep.md)

## Notes and Other Information
- This is a static (internal) function not exposed outside list.c
- Handles the distinction between inline initial_elements and dynamically allocated elements arrays
- Serves as the common implementation for both shallow (`list_free`) and deep (`list_free_deep`) freeing operations
- Uses PostgreSQL's `pfree()` function for memory deallocation rather than standard `free()`
- Part of PostgreSQL's memory management system for list data structures