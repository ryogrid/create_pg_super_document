# timestamp_cmp_internal

## Location
[src/backend/utils/adt/timestamp.c:2210-2215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2210-L2215)

## Overview
Internal comparison function that compares two Timestamp values and returns a three-way comparison result (-1, 0, or 1).

## Definition
```c
int timestamp_cmp_internal(Timestamp dt1, Timestamp dt2)
```

## Detailed Description
timestamp_cmp_internal is the core comparison function for PostgreSQL timestamp values. It implements a three-way comparison that returns -1 if the first timestamp is earlier than the second, 0 if they are equal, and 1 if the first timestamp is later than the second. This function serves as the foundation for all timestamp comparison operations and is shared between timestamp and timestamptz types. The implementation is straightforward, leveraging the fact that Timestamp values are stored as 64-bit integers representing microseconds since the PostgreSQL epoch.

## Parameters / Member Variables
- `dt1`: First Timestamp value to compare
- `dt2`: Second Timestamp value to compare

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp: PostgreSQL's internal timestamp data type

- Called from (representative examples):
  - [timestamp_eq](timestamp_eq.md): Equality comparison operator
  - [timestamp_ne](timestamp_ne.md): Inequality comparison operator  
  - [timestamp_lt](timestamp_lt.md): Less-than comparison operator
  - [timestamp_gt](timestamp_gt.md): Greater-than comparison operator
  - [timestamp_le](timestamp_le.md): Less-than-or-equal comparison operator
  - [timestamp_ge](timestamp_ge.md): Greater-than-or-equal comparison operator
  - [timestamp_cmp](timestamp_cmp.md): Public comparison function for sorting
  - [timestamp_fastcmp](timestamp_fastcmp.md): Fast comparison for internal use
  - [timestamp_smaller](timestamp_smaller.md): Returns the smaller of two timestamps
  - [timestamp_larger](timestamp_larger.md): Returns the larger of two timestamps
  - [generate_series_timestamp](../g/generate_series_timestamp.md): Used in timestamp series generation
  - timestamptz_cmp_internal: Comparison for timestamptz values
  - [date_cmp_timestamp_internal](../d/date_cmp_timestamp_internal.md): Cross-type comparison with dates

## Notes and Other Information
- This is a shared implementation used by both timestamp and timestamptz comparison operations
- The function relies on the internal representation of timestamps as 64-bit integers
- Returns standard three-way comparison semantics: negative, zero, or positive integer
- Performance-critical function as it's used extensively in sorting, indexing, and conditional operations
- The implementation assumes that Timestamp values are properly normalized and finite

## Simplified Source

```c
int timestamp_cmp_internal(Timestamp dt1, Timestamp dt2) {
    // Three-way comparison: -1 if dt1 < dt2, 0 if equal, 1 if dt1 > dt2
    return (dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0);
}
```