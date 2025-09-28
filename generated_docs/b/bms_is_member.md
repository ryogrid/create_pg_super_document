# bms_is_member

## Location
[src/backend/nodes/bitmapset.c:510-538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L510-L538)

## Overview
Tests whether a specific integer value is a member of a bitmap set by checking if the corresponding bit is set.

## Definition

```c
bool
bms_is_member(int x, const Bitmapset *a)
```
## Detailed Description
This function determines if the integer value `x` is present in the bitmap set `a` by calculating which word and bit position the value corresponds to, then checking if that bit is set. The function uses the WORDNUM and BITNUM macros to efficiently compute the word index and bit position within that word. It handles edge cases by returning an error for negative values (which are not allowed in bitmap sets) and returning false for NULL sets or when the value would be beyond the allocated words in the set.

## Parameters / Member Variables
- `x`: The integer value to test for membership (must be non-negative)
- `a`: The bitmap set to search in (can be NULL, representing an empty set)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation function for bitmap sets)
  - WORDNUM (macro to calculate word index from bit number)
  - BITNUM (macro to calculate bit position within word)
  - bitmapword (type for bitmap word storage)
  - elog (error logging function)
- Called from (representative examples):
  - [execute_attr_map_cols](../e/execute_attr_map_cols.md) (tuple conversion)
  - [HeapDetermineColumnsInfo](../H/HeapDetermineColumnsInfo.md) (heap access method)
  - [has_partition_attrs](../h/has_partition_attrs.md) (partitioning logic)
  - [ExecBuildUpdateProjection](../E/ExecBuildUpdateProjection.md) (executor expression building)
  - [index_unchanged_by_update](../i/index_unchanged_by_update.md) (index maintenance)

## Notes and Other Information
This is one of the most frequently used bitmap set functions in PostgreSQL, appearing throughout the codebase for testing membership of relation IDs, column numbers, attribute numbers, and other identifiers. The function is designed to be fast for the common case where the value is within the allocated range, using simple array indexing and bit masking. The error for negative values reflects the design constraint that bitmap sets only handle non-negative integers, which aligns with their typical use for representing sets of database object identifiers.

## Simplified Source

```c
// Simplified version of bms_is_member
bool bms_is_member(int x, const Bitmapset *a) {
    int wordnum, bitnum;

    Assert(bms_is_valid_set(a));

    // Negative values not allowed in bitmap sets
    if (x < 0)
        elog(ERROR, "negative bitmapset member not allowed");

    // Empty set has no members
    if (a == NULL)
        return false;

    // Calculate which word and bit position
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);

    // Check if bit position is beyond allocated words
    if (wordnum >= a->nwords)
        return false;

    // Test if the bit is set
    if ((a->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0)
        return true;
    return false;
}
```

Key simplifications made:
- Function is already efficient with simple bit manipulation
- Essential for testing membership in bitmap sets
- Handles edge cases (negative values, NULL sets, out-of-range values)