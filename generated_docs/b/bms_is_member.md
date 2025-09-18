# bms_is_member

## Location
src/backend/nodes/bitmapset.c: 510 - 538

## Overview
Tests whether a specific integer value is a member of a bitmap set by checking if the corresponding bit is set.

## Definition


## Detailed Description
This function determines if the integer value `x` is present in the bitmap set `a` by calculating which word and bit position the value corresponds to, then checking if that bit is set. The function uses the WORDNUM and BITNUM macros to efficiently compute the word index and bit position within that word. It handles edge cases by returning an error for negative values (which are not allowed in bitmap sets) and returning false for NULL sets or when the value would be beyond the allocated words in the set.

## Parameters / Member Variables
- `x`: The integer value to test for membership (must be non-negative)
- `a`: The bitmap set to search in (can be NULL, representing an empty set)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_valid_set (validation function for bitmap sets)
  - WORDNUM (macro to calculate word index from bit number)
  - BITNUM (macro to calculate bit position within word)
  - bitmapword (type for bitmap word storage)
  - elog (error logging function)
- Called from (representative examples):
  - execute_attr_map_cols (tuple conversion)
  - HeapDetermineColumnsInfo (heap access method)
  - has_partition_attrs (partitioning logic)
  - ExecBuildUpdateProjection (executor expression building)
  - index_unchanged_by_update (index maintenance)

## Notes and Other Information
This is one of the most frequently used bitmap set functions in PostgreSQL, appearing throughout the codebase for testing membership of relation IDs, column numbers, attribute numbers, and other identifiers. The function is designed to be fast for the common case where the value is within the allocated range, using simple array indexing and bit masking. The error for negative values reflects the design constraint that bitmap sets only handle non-negative integers, which aligns with their typical use for representing sets of database object identifiers.