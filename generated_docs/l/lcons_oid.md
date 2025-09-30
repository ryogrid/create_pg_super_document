# lcons_oid

## Location
[src/backend/nodes/list.c:531-560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L531-L560)

## Overview
The  function prepends an OID (Object Identifier) to the beginning of an existing OID list, creating a new list cell at the head position.

## Definition

```c
List *
lcons_oid(Oid datum, List *list)
```
## Detailed Description
This function is a specialized version of the generic  function, specifically designed for OID lists. It adds a new OID element to the front of an existing list, ensuring type safety by asserting that the input list is indeed an OID list. If the input list is NIL (empty), it creates a new OID list with the given datum as the first element. Otherwise, it creates a new head cell and inserts the OID at the beginning.

The function maintains list invariants and performs type checking to ensure data integrity. It's commonly used in PostgreSQL's catalog and namespace management where OID lists are frequently manipulated.

## Parameters / Member Variables
- : The OID value to be prepended to the list
- : The existing OID list to prepend to (can be NIL for empty list)

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates that the list is of OID type
  -  - Creates a new list when input is NIL
  -  - Creates a new cell at the head of existing list
  -  - Macro to set the first OID element in the list
  -  - Validates list consistency

- Called from (representative examples):
  -  (src/backend/catalog/namespace.c:4230, 4234)
  -  (src/backend/commands/indexcmds.c:3174)
  -  (src/backend/utils/adt/partitionfuncs.c:219)

## Notes and Other Information
- This function is part of PostgreSQL's list manipulation API specifically for OID types
- The function ensures type safety through the  assertion
- Returns the modified list with the new OID at the front
- Used extensively in namespace resolution and catalog operations where OID hierarchies are common
- The function handles both empty (NIL) and non-empty list cases appropriately

## Simplified Source

```c
List * lcons_oid(Oid datum, List *list) {
    // Ensure we're working with an OID list
    Assert(IsOidList(list));

    if (list == NIL) {
        // Create new OID list if input is empty
        list = new_list(T_OidList, 1);
    } else {
        // Add new cell at the head of existing list
        new_head_cell(list);
    }

    // Set the OID value at the front of the list
    linitial_oid(list) = datum;

    // Validate list consistency
    check_list_invariants(list);

    return list;
}
```