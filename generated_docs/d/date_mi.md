# date_mi

## Location
[src/backend/utils/adt/date.c:487-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L487-L503)

## Overview
Computes the difference between two dates in days, returning the result as an integer.

## Definition
```c
Datum date_mi(PG_FUNCTION_ARGS)
```

## Detailed Description
The `date_mi` function calculates the difference between two DateADT values and returns the result as a 32-bit integer representing the number of days. This function implements the subtraction operator for PostgreSQL date types. The function subtracts the second date from the first date (dateVal1 - dateVal2), so a positive result means the first date is later than the second, and a negative result means the first date is earlier.

The function includes error checking to prevent subtraction of infinite date values, as such operations would not produce meaningful results. When infinite dates are detected, the function raises an error with an appropriate error message.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): `DateADT` - The minuend date (date to subtract from)
  - Second argument (index 1): `DateADT` - The subtrahend date (date to subtract)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT` - Macro to extract DateADT arguments from function call
  - `DATE_NOT_FINITE` - Macro to test if date is infinite
  - `ereport` - PostgreSQL error reporting function
  - [errcode](../e/errcode.md) - Error code specification macro
  - [errmsg](../e/errmsg.md) - Error message specification macro
  - `PG_RETURN_INT32` - Macro to return a 32-bit integer result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator interface)

## Notes and Other Information
- This function implements the SQL date subtraction operator (date - date)
- Returns the difference in days as a signed 32-bit integer
- Throws ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error when attempting to subtract infinite dates
- The operation dateVal1 - dateVal2 gives positive results when dateVal1 is later than dateVal2
- Part of the PostgreSQL date arithmetic function family located in src/backend/utils/adt/date.c
- The result can be negative, indicating the first date is earlier than the second date