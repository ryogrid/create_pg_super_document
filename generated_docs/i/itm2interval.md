# itm2interval

## Location
[src/backend/utils/adt/timestamp.c:2077-2114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2077-L2114)

## Overview
Converts a human-readable interval time structure (struct pg_itm) back to a PostgreSQL Interval data type, with comprehensive overflow checking to ensure finite results.

## Definition


## Detailed Description
The  function reconstructs a PostgreSQL interval from its component parts stored in a pg_itm structure. The conversion process includes:

1. **Month Calculation**: Combines years and months into total months, checking for overflow
2. **Day Assignment**: Directly copies the day component
3. **Time Assembly**: Systematically combines time components (hours, minutes, seconds, microseconds) into total microseconds, with overflow checking at each step
4. **Overflow Protection**: Uses PostgreSQL's safe arithmetic functions (pg_mul_s64_overflow, pg_add_s64_overflow) to detect and prevent overflow
5. **Finite Check**: Ensures the resulting interval represents a finite (non-infinite) value

The function prioritizes safety and finite results, rejecting any computation that would produce overflow or infinite intervals.

## Parameters / Member Variables
- : Input struct pg_itm containing interval components (tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec, tm_usec)
- : Output Interval structure to populate with month, day, and time (microseconds) fields

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (safe 64-bit multiplication with overflow detection)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (safe 64-bit addition with overflow detection)
  - INTERVAL_NOT_FINITE (macro to check for infinite interval values)
  - MONTHS_PER_YEAR, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC (time constants)
- Called from (representative examples):
  - [timestamp_age](../t/timestamp_age.md) (age calculation between timestamps)
  - [timestamptz_age](../t/timestamptz_age.md) (age calculation between timestamptz values)
  - [interval_trunc](interval_trunc.md) (interval truncation operations)

## Notes and Other Information
- Returns 0 on success, -1 on overflow or infinite result
- Performs comprehensive overflow checking at every arithmetic operation
- Designed specifically for computations expected to produce finite results
- The function is the inverse of interval2itm and essential for interval construction
- Uses PostgreSQL's safe arithmetic primitives to prevent undefined behavior
- Month field is limited to 32-bit signed integer range (INT_MIN to INT_MAX)
- Any input combination that would result in infinite intervals is rejected as overflow