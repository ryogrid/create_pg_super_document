# lappend

## Location
[src/backend/nodes/list.c:339-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L339-L356)

## Overview
Appends a pointer to a PostgreSQL List data structure, returning a pointer to the modified list.

## Definition

```c
List *
lappend(List *list, void *datum)
```
## Detailed Description
The  function is a fundamental list manipulation function in PostgreSQL that appends a pointer element to the end of a List. It handles both the case where the list is initially NIL (empty) and when the list already contains elements. The function may or may not destructively modify the original list structure, so callers must always use the returned value rather than continuing to use the original list pointer.

When the input list is NIL, the function creates a new list with a single element. For existing lists, it adds a new tail cell and sets the data pointer. The function includes assertions to ensure the list is a pointer list (T_List type) and performs invariant checking for debugging purposes.

## Parameters / Member Variables
- `*list`: The List to append to, or NIL to create a new list
- `*datum`: A void pointer to the data to be appended to the list
## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList (assertion check for list type)
  - [new_list](../n/new_list.md) (creates new list when input is NIL)
  - [new_tail_cell](../n/new_tail_cell.md) (adds new cell to existing list)
  - llast (macro to access last element of list)
  - [check_list_invariants](../c/check_list_invariants.md) (debugging/validation function)

## Notes and Other Information
- This is one of the core list manipulation functions in PostgreSQL's node system
- Callers must use the return value, not the original list pointer, as the function may reallocate
- Only works with pointer lists (T_List), not integer or OID lists
- Part of PostgreSQL's custom linked list implementation optimized for memory management
- The function maintains list invariants and includes debugging checks

## Simplified Source

```c
List *
lappend(List *list, void *datum)
{
    // Verify this is a pointer list
    Assert(IsPointerList(list));

    // Handle empty list case - create new list
    if (list == NIL)
        list = new_list(T_List, 1);
    else
        // Add new cell to existing list
        new_tail_cell(list);

    // Set the data and validate
    llast(list) = datum;
    check_list_invariants(list);
    return list;
}
```