# bcTruelen

## Location
[src/backend/utils/adt/varchar.c:670-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L670-L675)

## Overview
Calculates the "true" length of a BpChar (blank-padded character) string by excluding trailing blank spaces.

## Definition
```c
static inline int bcTruelen(BpChar *arg)
```

## Detailed Description
This function serves as a convenient wrapper around `bpchartruelen` for calculating the effective length of a BpChar string, which represents CHAR(n) data types in PostgreSQL. The function extracts the data portion and size from the BpChar structure and delegates to `bpchartruelen` to perform the actual computation. The "true" length refers to the length of the string without counting trailing blank characters, which is important for CHAR type semantics where trailing spaces are considered padding and should be ignored in many operations.

## Parameters / Member Variables
- `arg`: Pointer to a BpChar structure containing the blank-padded character data

## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_ANY (macro to extract data portion from variable-length structure)
  - VARSIZE_ANY_EXHDR (macro to get size excluding header)
  - [bpchartruelen](bpchartruelen.md) (actual implementation for calculating true length)
- Called from (representative examples):
  - [bpcharlen](bpcharlen.md) (length function for CHAR type)
  - [bpchareq](bpchareq.md) (equality comparison for CHAR type)
  - [bpcharne](bpcharne.md) (inequality comparison for CHAR type)
  - [bpcharlt](bpcharlt.md)/bpcharle/bpchargt/bpcharge (ordering comparisons for CHAR type)
  - [bpcharcmp](bpcharcmp.md) (comparison function for CHAR type)
  - [bpchar_larger](bpchar_larger.md)/bpchar_smaller (min/max functions for CHAR type)
  - [hashbpchar](../h/hashbpchar.md)/hashbpcharextended (hashing functions for CHAR type)
  - [internal_bpchar_pattern_compare](../i/internal_bpchar_pattern_compare.md) (pattern matching for CHAR type)

## Notes and Other Information
- This function is declared as static inline for performance optimization since it's frequently called by comparison and utility functions
- The function is essential for proper CHAR type semantics where trailing spaces are ignored
- Works with PostgreSQL's variable-length data structures (VARDATA/VARSIZE macros)
- Used extensively throughout CHAR type operations including comparisons, hashing, and length calculations