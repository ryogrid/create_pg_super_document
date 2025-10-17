# timestamptz_bin

## Location
[src/backend/utils/adt/timestamp.c:4752-4825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4752-L4825)

## Overview
Bins a timestamp with time zone into a specified interval by calculating the start of the interval bin that contains the given timestamp.

## Definition
```c
Datum timestamptz_bin(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamptz_bin` function is the timezone-aware version of `timestamp_bin`. It takes a timestamp with timezone (TimestampTz) and bins it into a specified interval starting from a given origin point. This function is essential for time-series data aggregation where timezone-aware timestamps need to be grouped into regular intervals.

The function operates identically to `timestamp_bin` but works with TimestampTz values instead of plain Timestamp values. It performs the same validations and calculations:
1. Validates that inputs are finite and the stride is positive
2. Ensures the interval contains no month/year components  
3. Calculates the time difference between the timestamp and origin
4. Determines which interval bin contains the timestamp
5. Returns the start time of that bin

The binning uses floor division (rounding towards negative infinity) for consistent behavior across positive and negative time differences.

## Parameters / Member Variables
- `stride` (Interval*): The interval size for binning (e.g., '15 minutes', '1 hour')
- `timestamp` (TimestampTz): The timezone-aware timestamp to be binned
- `origin` (TimestampTz): The timezone-aware reference point from which intervals are calculated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_TIMESTAMPTZ  
  - TIMESTAMP_NOT_FINITE
  - INTERVAL_NOT_FINITE
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md)
  - IS_VALID_TIMESTAMP
  - PG_RETURN_TIMESTAMPTZ
  - USECS_PER_DAY
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This is the timezone-aware counterpart to `timestamp_bin`
- Uses the same core algorithm as `timestamp_bin` but operates on TimestampTz values
- Maintains the same restrictions: only supports intervals with days and time components, not months/years
- Uses microsecond precision internally for all calculations
- Implements comprehensive overflow checking to prevent arithmetic errors
- The binning behavior rounds towards negative infinity for consistency
- All timezone information is preserved in the input and output values
- Error handling covers the same edge cases as the non-timezone version

## Simplified Source

```c
Datum timestamptz_bin(PG_FUNCTION_ARGS) {
    Interval *stride = PG_GETARG_INTERVAL_P(0);
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
    TimestampTz origin = PG_GETARG_TIMESTAMPTZ(2);
    TimestampTz result, stride_usecs, tm_diff, tm_modulo, tm_delta;

    // Return infinite timestamp as-is
    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMPTZ(timestamp);

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

    PG_RETURN_TIMESTAMPTZ(result);
}
```