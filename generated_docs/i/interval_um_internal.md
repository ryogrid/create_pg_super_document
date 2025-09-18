# interval_um_internal

## Location
[src/backend/utils/adt/timestamp.c:3385-3404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3385-L3404)

## Overview
Negates (reverses the sign of) all components of an interval, handling special infinite values and overflow conditions.

## Definition
```c
static void interval_um_internal(const Interval *interval, Interval *result)
```

## Detailed Description
This is an internal static function that implements interval negation by reversing the sign of all three components of an interval (time, day, and month). The function handles special cases for infinite intervals, converting positive infinity to negative infinity and vice versa. For finite intervals, it performs safe arithmetic operations using PostgreSQL's overflow-checking functions to prevent integer overflow. If any overflow occurs, it raises an error with the message "interval out of range".

## Parameters / Member Variables
- `const Interval *interval`: Pointer to the input interval to be negated (read-only)
- `Interval *result`: Pointer to the output interval structure where the negated result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_IS_NOBEGIN (macro to check if interval represents negative infinity)
  - INTERVAL_NOEND (macro to set interval to positive infinity)
  - INTERVAL_IS_NOEND (macro to check if interval represents positive infinity) 
  - INTERVAL_NOBEGIN (macro to set interval to negative infinity)
  - INT64CONST (macro for 64-bit integer constant)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (safe 64-bit subtraction with overflow checking)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md) (safe 32-bit subtraction with overflow checking)
  - INTERVAL_NOT_FINITE (macro to check if interval is finite)
  - ereport (error reporting function)
- Called from (representative examples):
  - IA_TOTAL_COUNT (at src/backend/utils/adt/timestamp.c:98)
  - [timestamp_mi_interval](../t/timestamp_mi_interval.md) (at src/backend/utils/adt/timestamp.c:3172)
  - [timestamptz_mi_interval_internal](../t/timestamptz_mi_interval_internal.md) (at src/backend/utils/adt/timestamp.c:3330)
  - [interval_um](interval_um.md) (at src/backend/utils/adt/timestamp.c:3411)
  - [interval_mul](interval_mul.md) (at src/backend/utils/adt/timestamp.c:3595)
  - [interval_div](interval_div.md) (at src/backend/utils/adt/timestamp.c:3731)

## Notes and Other Information
- This is a static internal function, not directly accessible from SQL
- Handles three interval components: time (microseconds), day, and month
- Special handling for infinite intervals: NOBEGIN becomes NOEND and vice versa
- Uses PostgreSQL's safe arithmetic functions to prevent integer overflow
- Throws ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error if overflow is detected
- Located in src/backend/utils/adt/timestamp.c:3385-3404
- Used as a building block for various interval arithmetic operations including subtraction, multiplication, and division