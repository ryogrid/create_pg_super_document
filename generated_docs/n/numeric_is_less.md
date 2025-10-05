# numeric_is_less

## Location
[src/backend/utils/adt/dbsize.c:619-627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L619-L627)

## Overview
This static helper function compares two PostgreSQL Numeric values and returns true if the first value is less than the second.

## Definition

```c
static bool
numeric_is_less(Numeric a, Numeric b)
```
## Detailed Description
The numeric_is_less function provides a convenient boolean comparison interface for PostgreSQL Numeric values. It wraps the standard numeric_lt (numeric less than) function, converting the Numeric arguments to Datums and calling the comparison function through PostgreSQL's function call mechanism. This function abstracts away the complexity of the Datum conversion and function calling process, providing a simple boolean result for numeric comparisons.

## Parameters / Member Variables
- `a`: The first Numeric value (left operand of the comparison)
- `b`: The second Numeric value (right operand of the comparison)
## Dependencies
- Functions called/Symbols referenced:
  - [Numeric](../N/Numeric.md) (PostgreSQL numeric data type)
  - [NumericGetDatum](../N/NumericGetDatum.md) (converts Numeric values to Datum format)
  - DirectFunctionCall2 (calls a PostgreSQL function with two arguments)
  - [numeric_lt](numeric_lt.md) (the standard numeric less-than comparison function)
  - [DatumGetBool](../D/DatumGetBool.md) (extracts boolean result from Datum)
- Called from (representative examples):
  - [pg_size_pretty_numeric](../p/pg_size_pretty_numeric.md) (compares numeric values during size formatting)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Provides a cleaner interface for numeric comparison than calling numeric_lt directly
- Returns standard C boolean (true/false) rather than PostgreSQL's Datum boolean
- Handles all numeric comparison logic including special values, NaN, and different scales
- Used internally by size formatting functions that need to compare numeric thresholds
- The comparison follows PostgreSQL's standard numeric ordering rules

## Simplified Source

```c
static bool
numeric_is_less(Numeric a, Numeric b)
{
    Datum da = NumericGetDatum(a);
    Datum db = NumericGetDatum(b);

    return DatumGetBool(DirectFunctionCall2(numeric_lt, da, db));
}
```