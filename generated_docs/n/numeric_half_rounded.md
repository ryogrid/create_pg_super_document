# numeric_half_rounded

## Location
src/backend/utils/adt/dbsize.c: 638 - 659

## Overview
This static helper function performs half-rounding on a PostgreSQL Numeric value, implementing a custom rounding algorithm that adds or subtracts 1 before dividing by 2.

## Definition


## Detailed Description
The  function implements a specialized rounding operation that differs from standard mathematical rounding. It performs the following algorithm:
1. If the input number is greater than or equal to zero, it adds 1 to the number
2. If the input number is less than zero, it subtracts 1 from the number  
3. It then performs truncated division by 2 using 

This approach ensures that positive numbers are rounded up when the fractional part is 0.5 or greater, while negative numbers are rounded down (toward negative infinity) when the absolute fractional part is 0.5 or greater.

## Parameters / Member Variables
- : The input Numeric value to be half-rounded

## Dependencies
- Functions called/Symbols referenced:
  - NumericGetDatum: Converts Numeric to Datum
  - int64_to_numeric: Converts int64 to Numeric
  - DirectFunctionCall2: PostgreSQL function call interface
  - numeric_ge: Numeric greater-than-or-equal comparison
  - numeric_add: Numeric addition
  - numeric_sub: Numeric subtraction  
  - numeric_div_trunc: Numeric truncated division
  - DatumGetNumeric: Converts Datum to Numeric
- Called from (representative examples):
  - pg_size_pretty_numeric

## Notes and Other Information
This function is specifically designed for use in database size formatting operations. The half-rounding behavior is tailored for displaying human-readable size values where consistent rounding behavior is important for user experience. The function is located in .