# bms_del_members

## Location
src/backend/nodes/bitmapset.c: 1161 - 1229

## Overview
Removes all members from bitmap set 'a' that are also present in bitmap set 'b', recycling the left input bitmap set when possible.

## Definition


## Detailed Description
The bms_del_members function performs a set difference operation, removing from bitmap set 'a' all members that are also present in bitmap set 'b'. This is equivalent to computing A - B (A minus B) in set theory. The function is optimized to recycle the left input bitmap set rather than creating a new one, similar to bms_int_members.

The function uses bitwise operations to efficiently remove bits, applying the bitwise AND with the complement of b's bits (~b->words[i]). It handles different cases based on the relative sizes of the two bitmap sets and includes optimizations for trailing zero word removal when necessary.

## Parameters / Member Variables
- : The source bitmap set from which members will be removed (can be NULL)
- : The bitmap set containing members to be removed from 'a' (const, not modified, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_valid_set (validation of both inputs)
  - bms_copy_and_free (conditional memory management)
  - pfree (memory deallocation when result becomes empty)
  
- Called from (representative examples):
  - make_outerjoininfo (outer join information processing)
  - check_index_predicates (index predicate analysis)
  - get_join_domain_min_rels (join domain relation calculation)
  - finalize_plan (plan finalization)
  - build_join_rel (join relation construction)

## Notes and Other Information
- Returns NULL if 'a' is NULL or if the result becomes empty after deletion
- If 'b' is NULL, returns 'a' unchanged (nothing to delete)
- Modifies and potentially frees the left input bitmap set (a)
- Optimizes for cases where 'a' has more words than 'b' (no trailing word removal needed)
- When 'a' has fewer or equal words than 'b', tracks and removes trailing zero words
- Uses bitwise AND with complement (~b->words[i]) for efficient bit removal
- The right operand (b) is never modified (marked const)
- Supports conditional reallocation based on REALLOCATE_BITMAPSETS compile flag
- Extensively used in PostgreSQL's query optimization and join planning