# AdjustIntervalForTypmod

## Location
[src/backend/utils/adt/timestamp.c:1359-1538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1359-L1538)

## Overview
Adjusts an interval value for specified precision constraints in both YEAR to SECOND range and sub-second precision, supporting PostgreSQL's interval type modifier functionality.

## Definition

```c
static bool
AdjustIntervalForTypmod(Interval *interval, int32 typmod,
						Node *escontext)
```
## Detailed Description
This static function modifies an interval value to conform to type modifier constraints specified in PostgreSQL's INTERVAL type declarations. It handles two main aspects of interval adjustment:

1. **Range limiting**: Enforces field range restrictions (e.g., YEAR TO MONTH, DAY TO SECOND) by zeroing out fields outside the specified range
2. **Precision adjustment**: Rounds fractional seconds to the specified precision level (0-6 digits)

The function implements PostgreSQL's interpretation that fields to the right of the last specified field are zeroed out, while fields to the left remain valid. It uses static lookup tables (IntervalScales and IntervalOffsets) for efficient precision rounding operations.

For infinite intervals, no adjustments are applied. The function returns true on success or false on failure when using error context handling.

## Parameters / Member Variables
- `*interval`: Pointer to the Interval structure to be modified in-place
- `typmod`: Type modifier encoding both range and precision constraints (-1 means no constraints)
- `*escontext`: Error context node for soft error handling (NULL for hard errors)
## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_NOT_FINITE (macro for checking infinite intervals)
  - INTERVAL_RANGE (macro to extract range from typmod)
  - INTERVAL_PRECISION (macro to extract precision from typmod)
  - INTERVAL_MASK (macro for field masking)
  - INTERVAL_FULL_RANGE, INTERVAL_FULL_PRECISION (constants)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md), pg_sub_s64_overflow (overflow-safe arithmetic)
  - ereturn (error context return macro)
- Called from:
  - [interval_in](../i/interval_in.md) (interval input parsing)
  - [interval_recv](../i/interval_recv.md) (binary interval reception)
  - [interval_scale](../i/interval_scale.md) (interval scaling function)

## Notes and Other Information
- Implements PostgreSQL's post-8.4 behavior where only truncation/rounding of low-order fields occurs
- Uses microsecond-based time representation internally
- Static lookup tables optimize precision rounding for common cases
- Supports all standard SQL interval range specifications
- Error handling varies based on whether escontext is provided (soft vs hard error modes)

## Simplified Source

```c
static bool AdjustIntervalForTypmod(Interval *interval, int32 typmod, Node *escontext)
{
    // Precision lookup tables for rounding
    static const int64 IntervalScales[MAX_INTERVAL_PRECISION + 1] = {
        INT64CONST(1000000), INT64CONST(100000), INT64CONST(10000),
        INT64CONST(1000), INT64CONST(100), INT64CONST(10), INT64CONST(1)
    };
    static const int64 IntervalOffsets[MAX_INTERVAL_PRECISION + 1] = {
        INT64CONST(500000), INT64CONST(50000), INT64CONST(5000),
        INT64CONST(500), INT64CONST(50), INT64CONST(5), INT64CONST(0)
    };

    // No adjustment needed for infinite intervals or unspecified typmod
    if (INTERVAL_NOT_FINITE(interval) || typmod < 0)
        return true;

    int range = INTERVAL_RANGE(typmod);
    int precision = INTERVAL_PRECISION(typmod);

    // Apply range restrictions by zeroing fields outside the specified range
    if (range == INTERVAL_MASK(YEAR)) {
        interval->month = (interval->month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
        interval->day = 0;
        interval->time = 0;
    }
    else if (range == INTERVAL_MASK(MONTH)) {
        interval->day = 0;
        interval->time = 0;
    }
    else if (range == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH))) {
        interval->day = 0;
        interval->time = 0;
    }
    else if (range == INTERVAL_MASK(DAY)) {
        interval->time = 0;
    }
    else if (range == INTERVAL_MASK(HOUR)) {
        interval->time = (interval->time / USECS_PER_HOUR) * USECS_PER_HOUR;
    }
    else if (range == INTERVAL_MASK(MINUTE)) {
        interval->time = (interval->time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
    }
    // Additional range cases handled similarly...

    // Apply sub-second precision adjustment if specified
    if (precision != INTERVAL_FULL_PRECISION) {
        if (precision < 0 || precision > MAX_INTERVAL_PRECISION)
            return ereturn(escontext, false, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("interval(%d) precision must be between %d and %d",
                           precision, 0, MAX_INTERVAL_PRECISION)));

        // Round time value to specified precision using lookup tables
        if (interval->time >= 0) {
            if (pg_add_s64_overflow(interval->time, IntervalOffsets[precision], &interval->time))
                return ereturn(escontext, false, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("interval out of range")));
        } else {
            if (pg_sub_s64_overflow(interval->time, IntervalOffsets[precision], &interval->time))
                return ereturn(escontext, false, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("interval out of range")));
        }
        interval->time -= interval->time % IntervalScales[precision];
    }

    return true;
}
```