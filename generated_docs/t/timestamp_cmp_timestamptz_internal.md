# timestamp_cmp_timestamptz_internal

## Location
[src/backend/utils/adt/timestamp.c:2325-2345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2325-L2345)

## Overview
The timestamp_cmp_timestamptz_internal function performs cross-type comparison between timestamp and timestamptz values, handling timezone conversion and overflow conditions.

## Definition


## Detailed Description
This internal function implements the core logic for comparing timestamp (without timezone) values with timestamptz (with timezone) values. It first converts the timestamp to timestamptz using the session's timezone setting, handling potential overflow conditions. The function handles special cases for infinite timestamps (+/-infinity) and delegates to timestamptz_cmp_internal for the actual comparison once both values are in the same timezone representation. This function is crucial for PostgreSQL's cross-type comparison operations between timestamp types.

## Parameters / Member Variables
- : The timestamp value (without timezone) to compare
- : The timestamptz value (with timezone) to compare against

## Dependencies
- Functions called/Symbols referenced:
  - [timestamp2timestamptz_opt_overflow](timestamp2timestamptz_opt_overflow.md) (converts timestamp to timestamptz with overflow detection)
  - TIMESTAMP_IS_NOEND (checks for positive infinity)
  - TIMESTAMP_IS_NOBEGIN (checks for negative infinity)
  - timestamptz_cmp_internal (performs final comparison)
- Called from (representative examples):
  - [timestamp_eq_timestamptz](timestamp_eq_timestamptz.md)
  - [timestamp_ne_timestamptz](timestamp_ne_timestamptz.md)
  - [timestamp_lt_timestamptz](timestamp_lt_timestamptz.md)
  - [timestamp_gt_timestamptz](timestamp_gt_timestamptz.md)
  - [timestamp_le_timestamptz](timestamp_le_timestamptz.md)
  - [timestamp_ge_timestamptz](timestamp_ge_timestamptz.md)
  - [timestamp_cmp_timestamptz](timestamp_cmp_timestamptz.md)
  - [timestamptz_eq_timestamp](timestamptz_eq_timestamp.md)
  - [timestamptz_ne_timestamp](timestamptz_ne_timestamp.md)
  - [timestamptz_lt_timestamp](timestamptz_lt_timestamp.md)
  - [timestamptz_gt_timestamp](timestamptz_gt_timestamp.md)
  - [timestamptz_le_timestamp](timestamptz_le_timestamp.md)
  - [timestamptz_ge_timestamp](timestamptz_ge_timestamp.md)
  - [timestamptz_cmp_timestamp](timestamptz_cmp_timestamp.md)
  - [cmpTimestampToTimestampTz](../c/cmpTimestampToTimestampTz.md)

## Notes and Other Information
- This is an internal function that serves as the foundation for all cross-type timestamp/timestamptz comparison operations
- Handles overflow conditions that can occur during timezone conversion, returning appropriate comparison results for edge cases
- The overflow handling ensures correct behavior when timestamp values exceed the valid range for timestamptz representation
- Returns -1, 0, or +1 indicating less than, equal to, or greater than relationships respectively
- Critical for maintaining consistency in PostgreSQL's type system when comparing different timestamp types