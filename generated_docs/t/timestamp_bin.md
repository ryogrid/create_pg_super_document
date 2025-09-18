# timestamp_bin

## Location
[src/backend/utils/adt/timestamp.c:4547-4617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4547-L4617)

## Overview
Bins a timestamp into a specified interval by calculating the start of the interval bin that contains the given timestamp.

## Definition


## Detailed Description
The  function takes a timestamp and "bins" it into a specified interval starting from a given origin point. This is useful for time-series data aggregation where you want to group timestamps into regular intervals (e.g., every 15 minutes, every hour, etc.).

The function performs the following operations:
1. Validates that the input timestamp and origin are finite (not infinite/-infinite)
2. Validates that the stride interval is finite and positive
3. Ensures the interval contains no month or year components (only days and time)
4. Calculates the difference between the timestamp and origin
5. Determines which interval bin the timestamp falls into
6. Returns the start time of that bin

The binning uses floor division, meaning it rounds towards negative infinity, ensuring consistent behavior across positive and negative time differences.

## Parameters / Member Variables
-  (Interval*): The interval size for binning (e.g., '15 minutes', '1 hour')
-  (Timestamp): The timestamp to be binned
-  (Timestamp): The reference point from which intervals are calculated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_TIMESTAMP
  - TIMESTAMP_NOT_FINITE
  - INTERVAL_NOT_FINITE
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md)
  - IS_VALID_TIMESTAMP
  - PG_RETURN_TIMESTAMP
  - USECS_PER_DAY
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- The function only supports intervals containing days and time components; month/year intervals are not supported due to their variable lengths
- Uses microsecond precision internally for calculations
- Implements proper overflow checking to prevent arithmetic errors
- The binning behavior rounds towards negative infinity, which ensures consistent results regardless of whether the timestamp is before or after the origin
- Error handling covers invalid inputs like infinite values, zero/negative strides, and arithmetic overflow conditions