# bms_member_index

## Location
src/backend/nodes/bitmapset.c: 539 - 581

## Overview
Determines the 0-based index position of a specific member within a bitmap set, counting only the set bits that precede the target member.

## Definition


## Detailed Description
This function calculates the ordinal position of member `x` within the bitmap set `a`, where the position is determined by counting how many other members (set bits) appear before `x` in the bitmap. The function first verifies that `x` is actually a member of the set, returning -1 if it's not found. For valid members, it counts all set bits in the words preceding the target word, then counts set bits within the target word that come before the target bit position. The implementation uses efficient population count operations (bmw_popcount) to count set bits quickly.

## Parameters / Member Variables
- `a`: The bitmap set to search within (must be valid, not NULL for meaningful results)
- `x`: The integer value whose index position is to be determined (must be a member of the set)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation function for bitmap sets)
  - [bms_is_member](bms_is_member.md) (membership testing function)
  - WORDNUM (macro to calculate word index from bit number)
  - BITNUM (macro to calculate bit position within word)
  - bmw_popcount (population count function for bitmap words)
  - bitmapword (type for bitmap word storage)
- Called from (representative examples):
  - [clauselist_apply_dependencies](../c/clauselist_apply_dependencies.md) (statistics dependency analysis)
  - mcv_match_expression (most common values statistics)
  - mcv_get_match_bitmap (bitmap matching for statistics)

## Notes and Other Information
This function is primarily used in PostgreSQL's extended statistics system where the order of attributes or expressions within a bitmap needs to be determined for statistical calculations. The 0-based indexing makes it suitable for array indexing operations. The function is optimized to avoid unnecessary computation by first checking membership and by skipping population counts for zero words. The masking operation in the final word ensures that only bits preceding the target bit are counted, maintaining the correct 0-based index semantics.