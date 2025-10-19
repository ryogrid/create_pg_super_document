# date_pli

## Location
[src/backend/utils/adt/date.c:504-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L504-L527)

## Overview
Adds a number of days to a date, returning a new date value while handling both positive and negative day additions.

## Definition
```c
Datum date_pli(PG_FUNCTION_ARGS)
```

## Detailed Description
The `date_pli` function implements date arithmetic by adding a specified number of days to a given date. This function supports both forward and backward date calculations, handling positive numbers of days (moving forward in time) and negative numbers of days (moving backward in time). The function includes comprehensive error checking to prevent integer overflow and ensures the resulting date remains within PostgreSQL's valid date range.

Special handling is provided for infinite date values - when the input date is infinite (positive or negative infinity), the function returns the same infinite value unchanged, as arithmetic operations on infinity are not meaningful. The function also validates that the resulting date falls within the allowed range for PostgreSQL dates.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): `DateADT` - The base date to which days will be added
  - Second argument (index 1): `int32` - The number of days to add (can be positive or negative)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT` - Macro to extract DateADT argument from function call
  - `PG_GETARG_INT32` - Macro to extract 32-bit integer argument from function call
  - `DATE_NOT_FINITE` - Macro to test if date is infinite
  - `IS_VALID_DATE` - Macro to validate date is within allowed range
  - `ereport` - PostgreSQL error reporting function
  - [errcode](../e/errcode.md) - Error code specification macro
  - [errmsg](../e/errmsg.md) - Error message specification macro
  - `PG_RETURN_DATEADT` - Macro to return a DateADT result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator interface)

## Notes and Other Information
- This function implements the SQL date addition operator (date + integer)
- Returns infinite dates unchanged when input is infinite
- Performs overflow detection by checking if the addition result has the wrong sign relative to the input
- Validates the result is within PostgreSQL's valid date range using IS_VALID_DATE
- Throws ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error for overflow or out-of-range results
- Part of the PostgreSQL date arithmetic function family located in src/backend/utils/adt/date.c
- Supports both positive and negative day additions for flexible date calculations

## Simplified Source

```c
Datum date_pli(PG_FUNCTION_ARGS) {
    DateADT date = PG_GETARG_DATEADT(0);
    int32 days = PG_GETARG_INT32(1);

    // Infinite dates remain unchanged
    if (DATE_NOT_FINITE(date))
        PG_RETURN_DATEADT(date);

    // Add days to the date
    DateADT result = date + days;

    // Check for overflow and valid date range
    if ((days >= 0 ? (result < date) : (result > date)) || !IS_VALID_DATE(result))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("date out of range")));

    PG_RETURN_DATEADT(result);
}
```