# mbms_overlap_sets

## Location
src/backend/nodes/multibitmapset.c: 146 - 162

## Overview
Identifies which bitmapsets in two multibitmapsets have overlapping members and returns a bitmapset of the list indexes where overlaps occur.

## Definition

```c
Bitmapset *
mbms_overlap_sets(const List *a, const List *b)
```
## Detailed Description
This function compares two multibitmapsets (represented as Lists of Bitmapset structures) to find which corresponding pairs of Bitmapsets have overlapping members. The result is a single Bitmapset where each set bit represents the list index of a position where the corresponding Bitmapsets from the two input multibitmapsets overlap.

The function iterates through both lists simultaneously using the forboth macro, comparing each pair of corresponding Bitmapsets using bms_overlap. When an overlap is detected, it adds the current list index to the result Bitmapset using bms_add_member and foreach_current_index.

## Parameters / Member Variables
- : The first List representing a multibitmapset to compare (read-only)
- : The second List representing a multibitmapset to compare (read-only)

## Dependencies
- Functions called/Symbols referenced:
  - forboth
  - bms_overlap
  - bms_add_member
  - foreach_current_index
- Called from (representative examples):
  - reduce_outer_joins_pass2

## Notes and Other Information
- Returns a Bitmapset where each bit represents a list index with overlapping Bitmapsets
- The function stops at the end of the shorter list, which means it only checks positions that exist in both multibitmapsets
- Returns NULL if no overlaps are found
- This is primarily used in PostgreSQL's outer join reduction logic to identify which sets of variables have potential conflicts
- The result encodes positional information rather than the actual overlapping members
- Both input multibitmapsets are read-only and not modified