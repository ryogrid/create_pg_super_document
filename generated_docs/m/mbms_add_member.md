# mbms_add_member

## Location
[src/backend/nodes/multibitmapset.c:44-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/multibitmapset.c#L44-L70)

## Overview
Adds a new member to a multibitmapset by setting a specific bit in a specific Bitmapset within the List structure.

## Definition

```c
List *
mbms_add_member(List *a, int listidx, int bitidx)
```
## Detailed Description
This function adds a new member to a multibitmapset, which is represented as a List of Bitmapset structures. The function takes a list index to specify which Bitmapset within the List to modify, and a bit index to specify which bit to set within that Bitmapset. It's conceptually similar to bms_add_member but operates on the more complex multibitmapset data structure.

The function automatically extends the List with empty (NULL) elements if the specified listidx is beyond the current list length. It then retrieves the target Bitmapset, adds the specified bit using bms_add_member, and updates the List element with the modified Bitmapset.

## Parameters / Member Variables
- : The input List representing the multibitmapset to modify
- : Zero-based index of the List element (Bitmapset) to modify
- : Bit number to be set within the target Bitmapset

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth_cell](../l/list_nth_cell.md)
  - [bms_add_member](../b/bms_add_member.md)
- Called from (representative examples):
  - [find_nonnullable_vars_walker](../f/find_nonnullable_vars_walker.md)
  - [find_forced_null_vars](../f/find_forced_null_vars.md)

## Notes and Other Information
- Both listidx and bitidx must be non-negative; the function will throw an ERROR if negative values are provided
- The function automatically grows the List as needed by appending NULL elements
- Returns the modified List (which may be the same as the input or a new List if growth occurred)
- Part of the multibitmapset utility functions for managing complex bitmap structures in PostgreSQL's optimizer