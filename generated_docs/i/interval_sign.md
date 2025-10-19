# interval_sign

## Location
[src/backend/utils/adt/timestamp.c:2514-2522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2514-L2522)

## Overview
Determines the sign of an interval value, returning -1 for negative intervals, 0 for zero intervals, and 1 for positive intervals.

## Definition

```c
static int
interval_sign(const Interval *interval)
```
## Detailed Description
The  function is a static helper function that evaluates the sign of an interval data type. It internally uses  to convert the interval to a comparable 128-bit integer representation, then compares this value with zero using . This function is essential for interval arithmetic operations that need to determine the direction or sign of time spans.

## Parameters / Member Variables
- `*interval`: A constant pointer to an Interval struct representing the time span to evaluate
## Dependencies
- Functions called/Symbols referenced:
  - [interval_cmp_value](interval_cmp_value.md)
  - [int64_to_int128](int64_to_int128.md)
  - [int128_compare](int128_compare.md)
  - INT128 (data type)
- Called from (representative examples):
  - [interval_mul](interval_mul.md)
  - [in_range_timestamptz_interval](in_range_timestamptz_interval.md)
  - [in_range_timestamp_interval](in_range_timestamp_interval.md)
  - [in_range_interval_interval](in_range_interval_interval.md)
  - [generate_series_timestamp](../g/generate_series_timestamp.md)
  - [generate_series_timestamptz_internal](../g/generate_series_timestamptz_internal.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the timestamp.c file
- The function returns standard comparison result values: -1 (negative), 0 (zero), or 1 (positive)
- Used extensively in interval arithmetic operations and range functions
- Critical for determining the direction of time series generation and interval multiplication operations

## Simplified Source

```c
static int
interval_sign(const Interval *interval)
{
    // Convert interval to linear representation and compare with zero
    INT128 span = interval_cmp_value(interval);
    INT128 zero = int64_to_int128(0);

    // Return -1 for negative, 0 for zero, 1 for positive
    return int128_compare(span, zero);
}
```