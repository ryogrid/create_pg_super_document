# timestamp_cmp_internal

## Location
src/backend/utils/adt/timestamp.c: 2210 - 2215

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
  - timestamp_eq: Equality comparison operator
  - timestamp_ne: Inequality comparison operator  
  - timestamp_lt: Less-than comparison operator
  - timestamp_gt: Greater-than comparison operator
  - timestamp_le: Less-than-or-equal comparison operator
  - timestamp_ge: Greater-than-or-equal comparison operator
  - timestamp_cmp: Public comparison function for sorting
  - timestamp_fastcmp: Fast comparison for internal use
  - timestamp_smaller: Returns the smaller of two timestamps
  - timestamp_larger: Returns the larger of two timestamps
  - generate_series_timestamp: Used in timestamp series generation
  - timestamptz_cmp_internal: Comparison for timestamptz values
  - date_cmp_timestamp_internal: Cross-type comparison with dates

## Notes and Other Information
- This is a shared implementation used by both timestamp and timestamptz comparison operations
- The function relies on the internal representation of timestamps as 64-bit integers
- Returns standard three-way comparison semantics: negative, zero, or positive integer
- Performance-critical function as it's used extensively in sorting, indexing, and conditional operations
- The implementation assumes that Timestamp values are properly normalized and finite