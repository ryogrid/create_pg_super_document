# interval_finite

## Location
[src/backend/utils/adt/timestamp.c:2155-2167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2155-L2167)

## Overview
A PostgreSQL SQL function that checks whether an interval value is finite (not infinity or -infinity).

## Definition
```c
Datum interval_finite(PG_FUNCTION_ARGS)
```

## Detailed Description
The interval_finite function is a public PostgreSQL function that determines if a given interval is finite. It extracts an interval argument from the function call using PG_GETARG_INTERVAL_P and checks if it represents a finite value by using the INTERVAL_NOT_FINITE macro. The function returns true if the interval is finite (represents a measurable time duration), and false if it represents positive or negative infinity.

## Parameters / Member Variables
- Function argument 0: An interval value to be checked for finiteness

## Dependencies
- Functions called/Symbols referenced:
  - Interval (interval data type)
  - PG_GETARG_INTERVAL_P (macro to extract interval pointer argument)
  - INTERVAL_NOT_FINITE (macro to check if interval is infinite)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found (likely called via SQL function calls)

## Notes and Other Information
This is a public PostgreSQL function that can be called from SQL queries using the isfinite() function on interval values. Similar to timestamps, PostgreSQL intervals support special values including positive infinity ('infinity') and negative infinity ('-infinity') to represent unbounded time durations. This function provides a way to distinguish between finite interval values and these special infinite values. The function follows PostgreSQL's standard function calling conventions using the PG_FUNCTION_ARGS framework and uses pointer-based argument extraction for the interval type.

## Simplified Source

```c
Datum
interval_finite(PG_FUNCTION_ARGS)
{
    Interval *interval = PG_GETARG_INTERVAL_P(0);

    // Return true if interval is finite (not infinity or -infinity)
    PG_RETURN_BOOL(!INTERVAL_NOT_FINITE(interval));
}
```