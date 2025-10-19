# date_mii

## Location
[src/backend/utils/adt/date.c:528-563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L528-L563)

## Overview
Subtracts a specified number of days from a date value, returning a new date.

## Definition

```c
Datum
date_mii(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements date subtraction by subtracting an integer number of days from a DateADT value. This function handles PostgreSQL's internal date representation and includes comprehensive overflow checking and boundary validation. The function preserves infinite date values unchanged and performs range validation to ensure the result remains within PostgreSQL's supported date range.

## Parameters / Member Variables
- : The source date value from which days will be subtracted
- : The number of days to subtract (can be positive or negative)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT: Extracts DateADT argument from function call
  - PG_GETARG_INT32: Extracts integer argument from function call  
  - DATE_NOT_FINITE: Checks if date value is infinite
  - PG_RETURN_DATEADT: Returns DateADT result
  - IS_VALID_DATE: Validates date is within supported range
  - ereport: Reports errors with specified error codes
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- Handles integer overflow detection by checking if the subtraction operation changed the date value in an unexpected direction
- Preserves infinite date values (both positive and negative infinity) without modification
- Raises ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error when the result would be outside PostgreSQL's supported date range
- The function is typically used through SQL's date subtraction operator

## Simplified Source

```c
Datum date_mii(PG_FUNCTION_ARGS) {
    DateADT date = PG_GETARG_DATEADT(0);
    int32 days = PG_GETARG_INT32(1);

    // Infinite dates remain unchanged
    if (DATE_NOT_FINITE(date))
        PG_RETURN_DATEADT(date);

    // Subtract days from the date
    DateADT result = date - days;

    // Check for overflow and valid date range
    if ((days >= 0 ? (result > date) : (result < date)) || !IS_VALID_DATE(result))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("date out of range")));

    PG_RETURN_DATEADT(result);
}
```