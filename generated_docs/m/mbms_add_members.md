# mbms_add_members

## Location
[src/backend/nodes/multibitmapset.c:71-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/multibitmapset.c#L71-L99)

## Overview
Performs a UNION operation between two multibitmapsets by adding all members from the second set to the first set, modifying the first set in-place.

## Definition

```c
List *
mbms_add_members(List *a, const List *b)
```
## Detailed Description
This function implements a UNION operation for multibitmapsets, which are represented as Lists of Bitmapset structures. It adds all members from multibitmapset b to multibitmapset a, modifying a in-place. The function is analogous to bms_add_members but operates on the more complex multibitmapset data structure.

The function first extends List a with NULL elements if it's shorter than List b, then iterates through both lists simultaneously using the forboth macro. For each corresponding pair of Bitmapsets, it calls bms_add_members to perform the actual bitmap union operation and updates the element in List a.

## Parameters / Member Variables
- : The destination List representing the multibitmapset to be modified (left operand of UNION)
- : The source List representing the multibitmapset to add from (right operand of UNION, read-only)

## Dependencies
- Functions called/Symbols referenced:
  - forboth
  - bms_add_members
- Called from (representative examples):
  - reduce_outer_joins_pass2
  - find_nonnullable_vars_walker
  - find_forced_null_vars

## Notes and Other Information
- The operation modifies List a in-place and returns the modified List
- List a is automatically extended with NULL elements if it's shorter than List b
- The forboth macro stops at the end of the shorter list, but since a is extended to match b's length, all elements in b are processed
- This is a fundamental operation for combining multibitmapsets in PostgreSQL's query optimizer
- Used extensively in outer join reduction and null variable analysis