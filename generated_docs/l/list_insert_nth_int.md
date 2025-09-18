# list_insert_nth_int

## Location
src/backend/nodes/list.c: 453 - 466

## Overview
Inserts an integer value at a specified position in a PostgreSQL List that specifically contains integer elements, maintaining type safety and list structure.

## Definition
```c
List *list_insert_nth_int(List *list, int pos, int datum)
```

## Detailed Description
The `list_insert_nth_int` function is a type-safe variant of `list_insert_nth` specifically designed for Lists containing integer values. It inserts a new integer element at the specified position (0-based indexing) and shifts all following elements accordingly. The function enforces type safety by asserting that the target list contains only integer elements through `IsIntegerList` validation.

Like its generic counterpart, this function has O(n) time complexity proportional to the distance to the end of the list, as subsequent entries must be moved to accommodate the new element. It handles both empty lists (NIL) and existing integer lists while maintaining proper list invariants.

## Parameters / Member Variables
- `list`: The target integer List to insert into (can be NIL for empty list)
- `pos`: Zero-based position index where the new integer should be inserted
- `datum`: The integer value to be inserted into the list

## Dependencies
- Functions called/Symbols referenced:
  - `list_make1_int`: Creates a new single-element integer list (used for NIL case)
  - `IsIntegerList`: Validates that the list contains integer elements
  - `insert_new_cell`: Internal helper function to create and position a new list cell
  - `lfirst_int`: Macro to access the integer value of a list cell
  - `[check_list_invariants](../c/check_list_invariants.md)`: Debug function to verify list structural integrity

- Called from (representative examples):
  - `forfive`: Macro for five-way list iteration

## Notes and Other Information
- Type-safe version that only works with integer lists, verified by `IsIntegerList` assertion
- Asserts that position is valid (pos == 0 for NIL lists)
- Uses `lfirst_int` macro for type-safe integer access instead of generic `lfirst`
- Maintains list invariants through `check_list_invariants` in debug builds
- Time complexity is O(k) where k is the number of elements after insertion point
- Returns the modified list (same list object, not a new copy)
- Part of PostgreSQL's type-safe list API that prevents mixing different data types in lists