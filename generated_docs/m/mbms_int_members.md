# mbms_int_members

## Location
src/backend/nodes/multibitmapset.c: 100 - 125

## Overview
Performs an INTERSECT operation between two multibitmapsets by reducing the first set to its intersection with the second set, modifying the first set in-place.

## Definition

```c
List *
mbms_int_members(List *a, const List *b)
```
## Detailed Description
This function implements an INTERSECT operation for multibitmapsets, which are represented as Lists of Bitmapset structures. It reduces multibitmapset a to contain only the members that are also present in multibitmapset b, modifying a in-place. The function is analogous to bms_int_members but operates on the more complex multibitmapset data structure.

The function first truncates List a to match the length of List b (since any elements beyond b's length would have no corresponding intersection). Then it iterates through both lists simultaneously using the forboth macro. For each corresponding pair of Bitmapsets, it calls bms_int_members to perform the actual bitmap intersection operation and updates the element in List a.

## Parameters / Member Variables
- : The destination List representing the multibitmapset to be modified (left operand of INTERSECT)
- : The source List representing the multibitmapset to intersect with (right operand of INTERSECT, read-only)

## Dependencies
- Functions called/Symbols referenced:
  - list_truncate
  - forboth
  - bms_int_members
- Called from (representative examples):
  - find_nonnullable_vars_walker

## Notes and Other Information
- The operation modifies List a in-place and returns the modified List
- List a is truncated to match the length of List b before processing
- The forboth macro processes corresponding elements from both lists
- Elements in List a beyond the length of List b are automatically removed
- This is used in PostgreSQL's query optimizer for analyzing variable nullability constraints
- The intersection operation preserves only bits that are set in both corresponding Bitmapsets