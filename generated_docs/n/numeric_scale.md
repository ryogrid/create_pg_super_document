# numeric_scale

## Location
[src/backend/utils/adt/numeric.c:4138-4151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4138-L4151)

## Overview
Returns the scale of a numeric value, which is the count of decimal digits in the fractional part.

## Definition

```c
Datum
numeric_scale(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that extracts the scale (number of digits after the decimal point) from a numeric value. The function handles special numeric values (such as NaN or infinity) by returning NULL. For regular numeric values, it returns the scale as stored in the numeric data structure.

## Parameters / Member Variables
- The function uses the standard PostgreSQL function calling convention 
- Input: A single numeric value accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts numeric argument from function call
  -  - Checks if numeric value is special (NaN, infinity)
  -  - Extracts scale from numeric data structure
  -  - Returns integer result
  -  - Returns NULL for special values
- Called from (representative examples):
  -  - Used in money to numeric conversions
  -  - Used in numeric to money conversions

## Notes and Other Information
- Returns NULL for special numeric values (NaN, positive/negative infinity)
- The scale represents the number of digits to the right of the decimal point
- Used primarily in type conversion functions between numeric and money types
- Located in src/backend/utils/adt/numeric.c:4138-4151