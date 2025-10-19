# interval_cmp_value

## Location
[src/backend/utils/adt/timestamp.c:2483-2504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2483-L2504)

## Overview
Converts an Interval structure to a linear 128-bit integer representation for comparison purposes, normalizing months, days, and time components into a single comparable value.

## Definition
```c
static inline INT128 interval_cmp_value(const Interval *interval)
```

## Detailed Description
This function implements the core logic for interval comparison by converting an Interval structure (which contains separate month, day, and time fields) into a single linear representation expressed in microseconds. The conversion uses standardized assumptions: months are treated as 30 days, and days are treated as 24 hours. The function uses 128-bit arithmetic to avoid overflow when dealing with large interval values. This linear representation enables consistent comparison and ordering of interval values across different time units.

## Parameters / Member Variables
- `interval`: Pointer to an Interval structure containing:
  - `month`: Number of months in the interval
  - `day`: Number of days in the interval
  - `time`: Time component in microseconds

## Dependencies
- Functions called/Symbols referenced:
  - INT128 (128-bit integer type)
  - INT64CONST (macro for 64-bit constants)
  - [int64_to_int128](int64_to_int128.md) (conversion from 64-bit to 128-bit integer)
  - [int128_add_int64_mul_int64](int128_add_int64_mul_int64.md) (128-bit arithmetic operation)
  - USECS_PER_DAY (constant for microseconds per day)
- Called from:
  - [interval_cmp_internal](interval_cmp_internal.md) (internal interval comparison function)
  - [interval_sign](interval_sign.md) (determines sign of interval)
  - [interval_hash](interval_hash.md) (hashing function for intervals)
  - [interval_hash_extended](interval_hash_extended.md) (extended hashing function for intervals)

## Notes and Other Information
- Uses normalized time units: months = 30 days, days = 24 hours
- Requires 128-bit arithmetic to prevent overflow with large intervals
- The linear representation enables consistent ordering and comparison
- This is a static inline function for performance optimization
- Part of PostgreSQL's interval comparison infrastructure
- Located in src/backend/utils/adt/timestamp.c:2483-2504

## Simplified Source

```c
static inline INT128
interval_cmp_value(const Interval *interval)
{
    INT128 span;
    int64 days;

    // Convert months and days to total days (30 days per month)
    days = interval->month * INT64CONST(30);
    days += interval->day;

    // Start with the time component as base 128-bit value
    span = int64_to_int128(interval->time);

    // Add days converted to microseconds using 128-bit arithmetic
    int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

    return span;
}
```