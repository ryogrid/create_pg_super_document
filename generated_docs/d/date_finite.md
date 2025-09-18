# date_finite

## Location
[src/backend/utils/adt/date.c:459-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L459-L466)

## Overview
Tests whether a date value is finite (not infinity or negative infinity).

## Definition
```c
Datum date_finite(PG_FUNCTION_ARGS)
```

## Detailed Description
The `date_finite` function is a PostgreSQL built-in function that checks if a given date value is finite. It returns true if the date is a valid finite date, and false if the date represents positive or negative infinity. This function is part of PostgreSQL's date/time data type system and provides a way to test for special infinite date values that can be stored in the DateADT type.

The function uses the `DATE_NOT_FINITE` macro to perform the actual check and returns the negated result, effectively testing for finiteness rather than non-finiteness.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): `DateADT` - The date value to test for finiteness

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT` - Macro to extract DateADT argument from function call
  - `DATE_NOT_FINITE` - Macro to test if date is infinite
  - `PG_RETURN_BOOL` - Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- This function is exposed as a SQL-callable function for checking date finiteness
- Returns boolean true for finite dates, false for infinite dates
- Part of the PostgreSQL date/time function family located in src/backend/utils/adt/date.c
- The function handles both positive and negative infinity values through the DATE_NOT_FINITE macro