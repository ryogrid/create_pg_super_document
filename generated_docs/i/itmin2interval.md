# itmin2interval

## Location
[src/backend/utils/adt/timestamp.c:2115-2127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2115-L2127)

## Overview
Converts a pg_itm_in input structure to a PostgreSQL Interval data type, with special handling for infinite intervals and compatibility with pre-17 database formats.

## Definition


## Detailed Description
The  function converts interval input data from a pg_itm_in structure to PostgreSQL's internal Interval format. This function has special characteristics:

1. **Simple Conversion**: Performs basic field copying with minimal validation
2. **Month Calculation**: Combines years and months into total months with overflow checking
3. **Direct Assignment**: Directly copies day and time (microsecond) components without decomposition
4. **Infinite Interval Handling**: Unlike itm2interval, this function allows infinite results and treats them as valid rather than errors
5. **Compatibility Layer**: Designed to handle intervals from pre-PostgreSQL 17 databases that allowed extreme values

The function serves as a more permissive alternative to itm2interval, specifically for input processing where infinite intervals should be preserved rather than rejected.

## Parameters / Member Variables
- : Input struct pg_itm_in containing raw interval input data (tm_year, tm_mon, tm_mday, tm_usec)
- : Output Interval structure to populate with month, day, and time fields

## Dependencies
- Functions called/Symbols referenced:
  - MONTHS_PER_YEAR (constant for year/month conversion)
  - INT_MAX, INT_MIN (limits for month field validation)
- Called from (representative examples):
  - [interval_in](interval_in.md) (interval input parsing)
  - [pg_timezone_abbrevs](../p/pg_timezone_abbrevs.md) (timezone abbreviation functions)
  - [pg_timezone_names](../p/pg_timezone_names.md) (timezone name functions)

## Notes and Other Information
- Returns 0 on success, -1 only on month field overflow (not infinite intervals)
- Unlike itm2interval, infinite results are NOT treated as overflow
- Designed for compatibility with pre-PostgreSQL 17 databases
- Allows intervals with extreme values (INT_MIN/INT_MAX) to be converted to infinity
- The pg_itm_in structure contains pre-validated input data, so minimal checking is needed
- Month field overflow is the only error condition checked
- Time component (tm_usec) is copied directly without validation or component assembly
- This function prioritizes data preservation over strict validation