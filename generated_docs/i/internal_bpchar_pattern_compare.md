# internal_bpchar_pattern_compare

## Location
src/backend/utils/adt/varchar.c: 1119 - 1140

## Overview
Performs character-by-character comparison of BPCHAR values for pattern matching operations, supporting LIKE clause indexes without collation considerations.

## Definition
```c
static int internal_bpchar_pattern_compare(BpChar *arg1, BpChar *arg2)
```

## Detailed Description
This static function implements a specialized comparison algorithm for BPCHAR values that is used specifically for pattern matching operations and building indexes that support LIKE clauses. Unlike the standard BPCHAR comparison functions that are collation-aware, this function performs a pure binary (character-by-character) comparison using memcmp.

The function first determines the true length of both BPCHAR values (excluding trailing spaces) and then compares their character data byte-by-byte. If the characters differ within the common length, it returns the comparison result. If one string is a prefix of the other, the shorter string is considered smaller. This approach ensures consistent ordering for pattern matching operations regardless of locale settings.

The function is designed to be compatible with regular BPCHAR equality/inequality operators when used with "C" collation, making it suitable for building btree indexes that can efficiently support LIKE pattern matching queries.

## Parameters / Member Variables
- `arg1`: First BPCHAR value to compare
- `arg2`: Second BPCHAR value to compare  
- `result`: Integer result of memcmp comparison (-1, 0, or 1)
- `len1`: True length of first BPCHAR value (excluding trailing spaces)
- `len2`: True length of second BPCHAR value (excluding trailing spaces)

## Dependencies
- Functions called/Symbols referenced:
  - [bcTruelen](../b/bcTruelen.md) (determines true length of BPCHAR, excluding trailing spaces)
  - VARDATA_ANY (extracts character data from variable-length type)
  - memcmp (performs binary memory comparison)
  - Min (macro to get minimum of two values)
- Called from (representative examples):
  - [bpchar_pattern_lt](../b/bpchar_pattern_lt.md) (BPCHAR pattern less-than operator)
  - [bpchar_pattern_le](../b/bpchar_pattern_le.md) (BPCHAR pattern less-than-or-equal operator)
  - [bpchar_pattern_ge](../b/bpchar_pattern_ge.md) (BPCHAR pattern greater-than-or-equal operator)
  - [bpchar_pattern_gt](../b/bpchar_pattern_gt.md) (BPCHAR pattern greater-than operator)
  - [btbpchar_pattern_cmp](../b/btbpchar_pattern_cmp.md) (btree comparison function for pattern operations)

## Notes and Other Information
- Specifically designed for LIKE clause index support, not general string comparison
- Performs binary comparison without any collation or locale considerations
- Returns standard comparison semantics: negative if arg1 < arg2, zero if equal, positive if arg1 > arg2
- Compatible with regular BPCHAR operators when using "C" collation
- Static function, only accessible within the varchar.c compilation unit
- Essential for building efficient btree indexes on BPCHAR columns that need to support pattern matching queries
- The binary comparison approach ensures predictable and fast pattern matching index operations