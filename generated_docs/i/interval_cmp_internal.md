# interval_cmp_internal

## Location
[src/backend/utils/adt/timestamp.c:2505-2513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2505-L2513)

## Overview
Internal function that compares two Interval structures and returns an integer indicating their relative ordering (-1, 0, or 1).

## Definition
```c
static int interval_cmp_internal(const Interval *interval1, const Interval *interval2)
```

## Detailed Description
This function provides the core comparison logic for interval data types in PostgreSQL. It converts both input intervals to their linear 128-bit representations using `interval_cmp_value`, then performs a 128-bit integer comparison using `int128_compare`. The function returns a standard three-way comparison result: negative if interval1 < interval2, zero if they are equal, and positive if interval1 > interval2. This internal function serves as the foundation for all interval comparison operations.

## Parameters / Member Variables
- `interval1`: Pointer to the first Interval structure for comparison
- `interval2`: Pointer to the second Interval structure for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [interval_cmp_value](interval_cmp_value.md) (converts interval to linear 128-bit representation)
  - INT128 (128-bit integer type)
  - [int128_compare](int128_compare.md) (128-bit integer comparison function)
- Called from:
  - [interval_eq](interval_eq.md) (equality comparison operator)
  - [interval_ne](interval_ne.md) (not-equal comparison operator)
  - [interval_lt](interval_lt.md) (less-than comparison operator)
  - [interval_gt](interval_gt.md) (greater-than comparison operator)
  - [interval_le](interval_le.md) (less-than-or-equal comparison operator)
  - [interval_ge](interval_ge.md) (greater-than-or-equal comparison operator)
  - [interval_cmp](interval_cmp.md) (public comparison function)
  - [interval_smaller](interval_smaller.md) (returns smaller of two intervals)
  - [interval_larger](interval_larger.md) (returns larger of two intervals)
  - [in_range_interval_interval](in_range_interval_interval.md) (range checking function)

## Notes and Other Information
- Static function used internally within PostgreSQL's interval comparison system
- Foundation for all interval comparison operators (=, <>, <, >, <=, >=)
- Uses 128-bit arithmetic to handle large interval values without overflow
- Comparison is based on normalized linear representation (30-day months, 24-hour days)
- Located in src/backend/utils/adt/timestamp.c:2505-2513