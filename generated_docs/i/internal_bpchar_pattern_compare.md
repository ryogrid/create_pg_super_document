# internal_bpchar_pattern_compare

## Location
[src/backend/utils/adt/varchar.c:1119-1140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L1119-L1140)

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

## Simplified Source

```c
static int internal_bpchar_pattern_compare(BpChar *arg1, BpChar *arg2) {
    // Get true lengths (excluding trailing spaces)
    int len1 = bcTruelen(arg1);
    int len2 = bcTruelen(arg2);

    // Compare data byte-by-byte for the common length
    int result = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    if (result != 0) {
        return result;  // Different within common length
    }

    // Same content in common length, compare by length
    if (len1 < len2) return -1;      // arg1 is shorter
    if (len1 > len2) return 1;       // arg1 is longer
    return 0;                        // Exactly equal
}
```

**Key Points:**
- Binary comparison for LIKE clause index support (no collation)
- Uses `bcTruelen()` to exclude trailing spaces from comparison
- Compares data with `memcmp()` for the common length portion
- Shorter string is considered "less than" if content is otherwise equal
- Returns standard comparison result: <0, 0, or >0
- Compatible with "C" collation BPCHAR operators