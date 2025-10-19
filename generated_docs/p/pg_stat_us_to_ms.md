# pg_stat_us_to_ms

## Location
[src/backend/utils/adt/pgstatfuncs.c:1351-1356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1351-L1356)

## Overview
A static inline utility function that converts microsecond timing values to millisecond values for PostgreSQL statistics reporting.

## Definition

```c
static inline double
pg_stat_us_to_ms(PgStat_Counter val_ms)
```
## Detailed Description
This function performs a simple unit conversion from microseconds to milliseconds by multiplying the input value by 0.001. Despite the parameter name suggesting milliseconds (val_ms), this function actually converts microsecond values to milliseconds, which is a common pattern in PostgreSQL's statistics system where timing measurements are often stored in microseconds but reported in milliseconds for better readability.

## Parameters / Member Variables
- `val_ms`: A PgStat_Counter value representing time in microseconds to be converted to milliseconds
## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (type definition)
- Called from (representative examples):
  - [pg_stat_get_io](pg_stat_get_io.md)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the pgstatfuncs.c file and will be inlined by the compiler for performance
- The parameter name 'val_ms' is somewhat misleading as it actually expects microsecond values
- Returns a double precision floating-point result to preserve fractional millisecond precision
- Used internally by statistics functions that need to present timing data in millisecond units

## Simplified Source

```c
static inline double
pg_stat_us_to_ms(PgStat_Counter val_ms)
{
    // Convert microseconds to milliseconds by multiplying by 0.001
    return val_ms * (double) 0.001;
}
```