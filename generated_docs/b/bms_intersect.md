# bms_intersect

## Location
[src/backend/nodes/bitmapset.c:292-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L292-L345)

## Overview
Creates a new Bitmapset containing only members that exist in both input bitmapsets (set intersection operation).

## Definition


## Detailed Description
This function performs a bitwise intersection operation on two Bitmapsets, creating a new Bitmapset that contains only the bits that are set in both input sets. The function optimizes performance by copying the smaller input set first and then ANDing it with the larger set. It also performs important optimizations: it tracks the last non-zero word to trim trailing zeros, and it returns NULL if the intersection is empty (no common bits). Both input sets remain unmodified.

## Parameters / Member Variables
- : First input bitmapset (can be NULL)
- : Second input bitmapset (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation macro for input bitmapsets)
  - [bms_copy](bms_copy.md) (creates a copy of a bitmapset)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)

- Called from (representative examples):
  - [UpdateChangedParamSet](../U/UpdateChangedParamSet.md)
  - [classify_matching_subplans](../c/classify_matching_subplans.md)
  - [match_eclasses_to_foreign_key_col](../m/match_eclasses_to_foreign_key_col.md)
  - [have_unsafe_outer_join_ref](../h/have_unsafe_outer_join_ref.md)
  - [get_matching_part_pairs](../g/get_matching_part_pairs.md)
  - [create_lateral_join_info](../c/create_lateral_join_info.md)
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)
  - build_joinrel_tlist

## Notes and Other Information
- Returns NULL if either input is NULL, treating NULL as an empty set
- Returns NULL if the intersection is empty (no common bits between the sets)
- Uses an optimization strategy: copies the smaller set first to minimize work
- Performs bitwise AND operations on corresponding word pairs
- Automatically trims trailing zero words from the result to maintain optimal memory usage
- The result is either NULL or a newly allocated Bitmapset that must be freed by the caller
- Essential for query optimization operations that need to find common relation sets or parameter dependencies
- Used extensively in join planning, parameter analysis, and constraint processing