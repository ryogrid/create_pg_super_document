# bms_difference

## Location
src/backend/nodes/bitmapset.c: 346 - 411

## Overview
Creates a new Bitmapset containing members from the first set that are not present in the second set (set difference operation).

## Definition


## Detailed Description
This function performs a bitwise difference operation (A - B) on two Bitmapsets, creating a new Bitmapset that contains only the bits that are set in the first set but not in the second set. The function includes several optimizations: it pre-checks if the result would be empty using bms_nonempty_difference() to avoid unnecessary allocation, handles cases where the first set has more words than the second efficiently, and trims trailing zero words when necessary. The operation uses bitwise AND with the complement of the second set (~b->words[i]).

## Parameters / Member Variables
- : First input bitmapset (minuend - what to subtract from, can be NULL)
- : Second input bitmapset (subtrahend - what to subtract, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_valid_set (validation macro for input bitmapsets)
  - bms_copy (creates a copy of a bitmapset)
  - bms_nonempty_difference (checks if difference would be non-empty)

- Called from (representative examples):
  - add_child_rel_equivalences
  - check_index_predicates
  - add_paths_to_joinrel
  - have_unsafe_outer_join_ref
  - remove_useless_groupby_columns
  - finalize_plan
  - make_restrictinfo_internal
  - pull_varnos_walker
  - examine_variable

## Notes and Other Information
- Returns NULL if the first input is NULL (nothing to subtract from)
- Returns a copy of the first input if the second input is NULL (nothing to subtract)
- Optimizes for the common case of empty results by pre-checking with bms_nonempty_difference()
- Handles different word lengths efficiently: no trailing zero removal needed when 'a' has more words than 'b'
- When both sets have the same length or 'b' is longer, tracks the last non-zero word for trimming
- Uses bitwise AND with complement (~) operation to remove bits: result->words[i] &= ~b->words[i]
- The result is either NULL or a newly allocated Bitmapset that must be freed by the caller
- Essential for query optimization operations that need to exclude certain relations or parameters
- Widely used in join planning, equivalence class processing, and constraint analysis