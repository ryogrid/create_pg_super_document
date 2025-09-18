# list_insert_nth_oid

## Location
[src/backend/nodes/list.c:467-494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L467-L494)

## Overview
Inserts an Object Identifier (Oid) value at a specified position in a PostgreSQL List that specifically contains Oid elements, maintaining type safety and list structure.

## Definition
```c
List *list_insert_nth_oid(List *list, int pos, Oid datum)
```

## Detailed Description
The `list_insert_nth_oid` function is a type-safe variant of `list_insert_nth` specifically designed for Lists containing Object Identifier (Oid) values. Oids are fundamental to PostgreSQL's internal object management system, used to uniquely identify database objects like tables, functions, types, and other system entities. This function inserts a new Oid element at the specified position (0-based indexing) and shifts all following elements accordingly.

The function enforces type safety by asserting that the target list contains only Oid elements through `IsOidList` validation. Like other insertion functions, it has O(n) time complexity proportional to the distance to the end of the list and maintains proper list invariants throughout the operation.

## Parameters / Member Variables
- `list`: The target Oid List to insert into (can be NIL for empty list)
- `pos`: Zero-based position index where the new Oid should be inserted
- `datum`: The Oid value to be inserted into the list

## Dependencies
- Functions called/Symbols referenced:
  - `list_make1_oid`: Creates a new single-element Oid list (used for NIL case)
  - `IsOidList`: Validates that the list contains Oid elements
  - `insert_new_cell`: Internal helper function to create and position a new list cell
  - `lfirst_oid`: Macro to access the Oid value of a list cell
  - [check_list_invariants](../c/check_list_invariants.md): Debug function to verify list structural integrity

- Called from (representative examples):
  - `forfive`: Macro for five-way list iteration

## Notes and Other Information
- Type-safe version that only works with Oid lists, verified by `IsOidList` assertion
- Oid (Object Identifier) is a fundamental PostgreSQL type for referencing database objects
- Asserts that position is valid (pos == 0 for NIL lists)
- Uses `lfirst_oid` macro for type-safe Oid access instead of generic `lfirst`
- Maintains list invariants through `check_list_invariants` in debug builds
- Time complexity is O(k) where k is the number of elements after insertion point
- Returns the modified list (same list object, not a new copy)
- Part of PostgreSQL's type-safe list API that prevents mixing different data types in lists
- Commonly used in query planning and execution for managing object references