# list_make2_impl

## Location
[src/backend/nodes/list.c:246-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L246-L256)

## Overview
A convenience function that creates a new List containing exactly two elements, providing the implementation for the list_make2 family of macros.

## Definition
```c
List *list_make2_impl(NodeTag t, ListCell datum1, ListCell datum2)
```

## Detailed Description
This function creates a new List of the specified type with exactly two elements. It allocates space for two cells using new_list(), assigns the provided data to the first and second elements in order, validates the resulting list structure with check_list_invariants(), and returns the completed list. The function serves as the underlying implementation for type-specific macros like list_make2(), list_make2_int(), list_make2_oid(), and list_make2_xid(), which provide type safety and convenience for creating two-element lists of various data types.

## Parameters / Member Variables
- `t`: The NodeTag specifying the type of list to create (T_List, T_IntList, T_OidList, or T_XidList)
- `datum1`: The ListCell containing the data to be stored as the first element of the list
- `datum2`: The ListCell containing the data to be stored as the second element of the list

## Dependencies
- Functions called/Symbols referenced:
  - [new_list](../n/new_list.md) (allocates and initializes List structure)
  - [check_list_invariants](../c/check_list_invariants.md) (validates list consistency)

- Called from (representative examples):
  - list_make2 (macro for generic pointer lists)
  - list_make2_int (macro for integer lists)
  - list_make2_oid (macro for OID lists)
  - list_make2_xid (macro for XID lists)
  - forfive (macro for list iteration)

## Notes and Other Information
- Part of the list_makeN_impl family for creating lists with N elements efficiently
- Typically called through type-specific macros rather than directly
- Elements are stored in the order provided: datum1 at index 0, datum2 at index 1
- The function ensures list invariants are maintained by calling check_list_invariants()
- Provides efficient creation of two-element lists without requiring multiple append operations
- The resulting list is immediately valid and ready for use or further modification
- More efficient than creating an empty list and calling lappend() twice

## Simplified Source

```c
List *list_make2_impl(NodeTag t, ListCell datum1, ListCell datum2) {
    // Create new list with capacity for 2 elements
    List *list = new_list(t, 2);

    // Store the two elements in order
    list->elements[0] = datum1;
    list->elements[1] = datum2;

    // Validate the list structure
    check_list_invariants(list);
    return list;
}
```