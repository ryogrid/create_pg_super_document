# timestamp_mi

## Location
[src/backend/utils/adt/timestamp.c:2786-2879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2786-L2879)

## Overview
Computes the interval between two timestamps by subtracting the second timestamp from the first, implementing the PostgreSQL timestamp subtraction operator.

## Definition
```c
Datum timestamp_mi(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the time difference between two timestamps and returns it as an Interval. It handles both finite and infinite timestamps, with special logic for infinity arithmetic. The function performs overflow checking and applies hour justification to the result.

The function handles several edge cases:
1. Infinite timestamp arithmetic - treats "infinity - infinity" as an error
2. Overflow detection when subtracting timestamp values
3. Automatic hour justification using interval_justify_hours to normalize the result

The implementation includes a documented workaround that maintains backward compatibility with existing regression tests, even though the behavior may not be entirely correct in some timezone edge cases.

## Parameters / Member Variables
- `dt1`: First timestamp (minuend) from PG_GETARG_TIMESTAMP(0)
- `dt2`: Second timestamp (subtrahend) from PG_GETARG_TIMESTAMP(1)
- `result`: Resulting interval representing dt1 - dt2

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (PostgreSQL function call interface macro)
  - TIMESTAMP_NOT_FINITE, TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND (infinity checking macros)
  - INTERVAL_NOBEGIN, INTERVAL_NOEND (infinity setting macros for intervals)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (safe 64-bit subtraction with overflow detection)
  - [interval_justify_hours](../i/interval_justify_hours.md) (normalizes hours to days conversion)
  - DirectFunctionCall1, DatumGetIntervalP, IntervalPGetDatum (PostgreSQL function call utilities)
  - PG_RETURN_INTERVAL_P (PostgreSQL return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Implements the PostgreSQL timestamp minus (-) operator
- Contains documented workaround for timezone edge cases to maintain regression test compatibility
- Automatically justifies hours to ensure reasonable interval representation
- Handles all combinations of finite and infinite timestamps with appropriate error checking
- Sets month and day fields to 0, focusing only on time difference
- Uses 64-bit arithmetic with overflow protection
- Located at src/backend/utils/adt/timestamp.c:2786-2879

## Simplified Source

```c
Datum timestamp_mi(PG_FUNCTION_ARGS)
{
    // Extract timestamp arguments
    Timestamp timestamp1 = PG_GETARG_TIMESTAMP(0);
    Timestamp timestamp2 = PG_GETARG_TIMESTAMP(1);
    Interval *result = (Interval *) palloc(sizeof(Interval));

    // Handle infinite timestamps - error on "infinity - infinity"
    if (TIMESTAMP_NOT_FINITE(timestamp1) || TIMESTAMP_NOT_FINITE(timestamp2)) {
        if ((TIMESTAMP_IS_NOBEGIN(timestamp1) && TIMESTAMP_IS_NOBEGIN(timestamp2)) ||
            (TIMESTAMP_IS_NOEND(timestamp1) && TIMESTAMP_IS_NOEND(timestamp2))) {
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("interval out of range")));
        }
        // Set appropriate infinity result based on operands
        // (detailed infinity logic omitted for brevity)
        PG_RETURN_INTERVAL_P(result);
    }

    // Compute difference with overflow checking
    if (pg_sub_s64_overflow(timestamp1, timestamp2, &result->time)) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("interval out of range")));
    }

    // Set month and day to 0 (only time difference)
    result->month = 0;
    result->day = 0;

    // Apply hour justification for proper interval normalization
    result = DatumGetIntervalP(DirectFunctionCall1(interval_justify_hours,
                                                  IntervalPGetDatum(result)));

    PG_RETURN_INTERVAL_P(result);
}
```