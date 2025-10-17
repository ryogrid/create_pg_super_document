# timestamp_bin

## Location
[src/backend/utils/adt/timestamp.c:4547-4617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4547-L4617)

## Overview
Bins a timestamp into a specified interval by calculating the start of the interval bin that contains the given timestamp.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
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

## Simplified Source

```c
Datum timestamp_bin(PG_FUNCTION_ARGS) {
    Interval *stride = PG_GETARG_INTERVAL_P(0);
    Timestamp timestamp = PG_GETARG_TIMESTAMP(1);
    Timestamp origin = PG_GETARG_TIMESTAMP(2);
    Timestamp result, stride_usecs, tm_diff, tm_modulo, tm_delta;

    // Return infinite timestamp as-is
    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMP(timestamp);

    // Validate inputs
    if (TIMESTAMP_NOT_FINITE(origin))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("origin out of range")));

    if (INTERVAL_NOT_FINITE(stride))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("timestamps cannot be binned into infinite intervals")));

    if (stride->month != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("timestamps cannot be binned into intervals containing months or years")));

    // Convert interval to microseconds
    if (pg_mul_s64_overflow(stride->day, USECS_PER_DAY, &stride_usecs) ||
        pg_add_s64_overflow(stride_usecs, stride->time, &stride_usecs))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("interval out of range")));

    if (stride_usecs <= 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("stride must be greater than zero")));

    // Calculate difference from origin
    if (pg_sub_s64_overflow(timestamp, origin, &tm_diff))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("interval out of range")));

    // Bin the timestamp: find start of interval containing timestamp
    tm_modulo = tm_diff % stride_usecs;
    tm_delta = tm_diff - tm_modulo;
    result = origin + tm_delta;

    // Round towards -infinity for negative remainders
    if (tm_modulo < 0) {
        if (pg_sub_s64_overflow(result, stride_usecs, &result) ||
            !IS_VALID_TIMESTAMP(result))
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));
    }

    PG_RETURN_TIMESTAMP(result);
}
```