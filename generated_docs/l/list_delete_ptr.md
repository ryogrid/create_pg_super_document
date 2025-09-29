# list_delete_ptr

## Location
[src/backend/nodes/list.c:872-890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L872-L890)

## Overview
Deletes the first cell in a list that contains a pointer exactly matching the specified pointer value, using simple pointer equality comparison.

## Definition
```c
List *list_delete_ptr(List *list, void *datum)
```

## Detailed Description
This function is similar to `list_delete()` but uses simple pointer equality (`==`) instead of PostgreSQL's generic `equal()` function for comparison. It searches through a pointer list linearly to find the first cell containing a pointer that exactly matches the specified datum pointer and removes it.

This is more efficient than `list_delete()` when you want to remove a specific object by its memory address rather than by its content value. The function performs a linear search from the beginning of the list, making it O(n) in time complexity. If a matching pointer is found, the cell is removed using `list_delete_cell()`. If no match is found, the original list is returned unmodified.

## Parameters / Member Variables
- `list`: The List to search and potentially modify  
- `datum`: The specific pointer value to search for and remove from the list

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList (macro for type checking)
  - [check_list_invariants](../c/check_list_invariants.md)
  - [list_delete_cell](list_delete_cell.md)
- Called from (representative examples):
  - [FreeExprContext](../F/FreeExprContext.md) (src/backend/executor/execUtils.c:425)
  - [remove_join_clause_from_rels](../r/remove_join_clause_from_rels.md) (src/backend/optimizer/util/joininfo.c:181)
  - [pa_free_worker_info](../p/pa_free_worker_info.md) (src/backend/replication/logical/applyparallelworker.c:613)

## Notes and Other Information
- Only works with pointer lists, not integer or OID lists
- Uses simple pointer equality (`ptr1 == ptr2`) rather than deep content comparison
- More efficient than `list_delete()` when removing objects by memory address
- Linear search makes it unsuitable for long lists - O(n) time complexity
- Removes only the first matching pointer, not all matches
- Returns the original list unchanged if no matching pointer is found
- Commonly used in resource cleanup and object reference management

## Simplified Source

```c
List *list_delete_ptr(List *list, void *datum) {
    ListCell *cell;

    Assert(IsPointerList(list));
    check_list_invariants(list);

    // Search for matching pointer
    foreach(cell, list) {
        if (lfirst(cell) == datum)
            return list_delete_cell(list, cell);
    }

    // No match found - return original list
    return list;
}
```