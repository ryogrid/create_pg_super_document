# date_mii

## Location
src/backend/utils/adt/date.c: 528 - 563

## Overview
Subtracts a specified number of days from a date value, returning a new date.

## Definition


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