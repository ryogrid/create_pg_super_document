# numeric_absolute

## Location
[src/backend/utils/adt/dbsize.c:628-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L628-L637)

## Overview
This static helper function calculates and returns the absolute value of a PostgreSQL Numeric value.

## Definition

```c
static Numeric
numeric_absolute(Numeric n)
```
## Detailed Description
The numeric_absolute function computes the absolute value of a PostgreSQL Numeric data type. It serves as a wrapper around the standard numeric_abs function, providing a convenient interface for obtaining absolute values while handling the Datum conversion process automatically. The function preserves the precision and scale of the input numeric value while ensuring the result is always non-negative. It handles all special numeric values including positive/negative infinity and NaN according to PostgreSQL's standard numeric rules.

## Parameters / Member Variables
- : The input Numeric value whose absolute value is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - Numeric (PostgreSQL numeric data type)
  - [NumericGetDatum](../N/NumericGetDatum.md) (converts Numeric to Datum format)
  - DirectFunctionCall1 (calls a PostgreSQL function with one argument)
  - [numeric_abs](numeric_abs.md) (the standard numeric absolute value function)
  - [DatumGetNumeric](../D/DatumGetNumeric.md) (extracts Numeric result from Datum)
- Called from (representative examples):
  - [pg_size_pretty_numeric](../p/pg_size_pretty_numeric.md) (calculates absolute values during size formatting)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Provides a cleaner interface for absolute value calculation than calling numeric_abs directly
- Preserves the original numeric precision and scale
- Handles special values like NaN and infinity according to PostgreSQL standards
- Returns a new Numeric value rather than modifying the input
- Used internally by size formatting functions that need to work with absolute values for unit selection
- The function follows PostgreSQL's memory management conventions for Numeric values