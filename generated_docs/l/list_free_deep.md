# list_free_deep

## Location
[src/backend/nodes/list.c:1560-1572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1560-L1572)

## Overview
Frees all memory associated with a List structure including both the list itself and all the objects pointed to by the list elements.

## Definition

```c
void
list_free_deep(List *list)
```
## Detailed Description
This function performs a deep free operation on a PostgreSQL List structure. Unlike `list_free()`, it not only deallocates the memory used by the list structure itself but also frees every object that the list elements point to. This function should only be used when the list contains pointers to `palloc()`'d memory regions that are owned exclusively by the list.

The function includes an assertion to verify that the list is indeed a pointer list using `IsPointerList()`, since deep freeing only makes sense for lists containing pointers. It then delegates to `list_free_private(list, true)` where the true parameter indicates that both the list structure and the pointed-to elements should be freed.

This is the appropriate function to use when the list owns all the objects it points to and they should be deallocated along with the list.

## Parameters / Member Variables
- `list`: The List structure to free. Must be a pointer list (containing pointers to palloc'd memory). Both the list and all pointed-to objects will be deallocated.

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList
  - [list_free_private](list_free_private.md)
- Called from (representative examples):
  - [gistbufferinginserttuples](../g/gistbufferinginserttuples.md)
  - [CreateDatabaseUsingWalLog](../C/CreateDatabaseUsingWalLog.md)
  - [CloseTableList](../C/CloseTableList.md)
  - [RelationDestroyRelation](../R/RelationDestroyRelation.md)
  - [load_libraries](load_libraries.md)

## Notes and Other Information
- This function performs a deep free - both the list structure and all pointed-to elements are freed
- Only works with pointer lists (lists containing pointers to palloc'd regions)
- The assertion `IsPointerList(list)` ensures the list is appropriate for deep freeing
- The caller should set the list pointer to NIL after calling this function for safety
- Used when the list owns all the objects it points to and they should be cleaned up together
- Counterpart to `list_free()` which performs only shallow freeing
- Essential for preventing memory leaks when lists contain dynamically allocated objects
- Part of PostgreSQL's comprehensive memory management system

## Simplified Source

```c
// Simplified version of list_free_deep
void list_free_deep(List *list) {
    // Verify this is a pointer list (only makes sense for deep free)
    Assert(IsPointerList(list));

    // Perform deep free: free both list structure and pointed-to objects
    if (list == NIL) {
        return; // Nothing to do for empty list
    }

    // Free each object pointed to by list elements
    for (int i = 0; i < list->length; i++) {
        pfree(lfirst(&list->elements[i]));
    }

    // Free the list's element array if it was dynamically allocated
    if (list->elements != list->initial_elements) {
        pfree(list->elements);
    }

    // Free the list structure itself
    pfree(list);
}
```

Key simplifications made:
- Inlined the `list_free_private` logic to show the complete operation
- Removed `check_list_invariants` call for clarity
- Added descriptive comments for each step
- Focused on the main execution path
- Maintained the essential algorithm and safety checks