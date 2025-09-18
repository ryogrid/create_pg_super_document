# bms_union

## Location
src/backend/nodes/bitmapset.c: 251 - 291

## Overview
Creates a new Bitmapset containing all members from both input bitmapsets (set union operation).

## Definition


## Detailed Description
This function performs a bitwise union operation on two Bitmapsets, creating a new Bitmapset that contains all bits that are set in either input set. The function optimizes performance by copying the larger input set first and then ORing the smaller set into it. This approach minimizes the number of word-by-word operations needed. Both input sets remain unmodified, making this a pure functional operation.

## Parameters / Member Variables
- : First input bitmapset (can be NULL)
- : Second input bitmapset (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_valid_set (validation macro for input bitmapsets)
  - bms_copy (creates a copy of a bitmapset)

- Called from (representative examples):
  - ExecGetAllUpdatedCols
  - generate_join_implied_equalities
  - make_join_rel
  - join_is_removable
  - deconstruct_jointree
  - finalize_plan
  - build_join_rel
  - get_joinrel_parampathinfo

## Notes and Other Information
- Safely handles NULL inputs by treating NULL as an empty set
- Returns a copy of the non-NULL input when the other input is NULL
- Uses an optimization strategy: copies the larger set first, then unions the smaller one
- Performs bitwise OR operations on corresponding word pairs for efficiency
- Extensively used in PostgreSQL's query optimizer for combining relation sets and join conditions
- The result is a newly allocated Bitmapset that must be freed by the caller using bms_free()
- Critical for join planning, constraint processing, and relation management operations