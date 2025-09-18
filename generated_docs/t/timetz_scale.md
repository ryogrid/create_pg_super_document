# timetz_scale

## Location
src/backend/utils/adt/date.c: 2425 - 2442

## Overview
Adjusts a TIMETZ (time with time zone) value to conform to a specified precision scale factor, typically used when casting between TIMETZ types with different precision specifications.

## Definition


## Detailed Description
The `timetz_scale` function is responsible for adjusting the precision of TIMETZ values according to a specified type modifier (typmod). This function is commonly used by PostgreSQL's type system when performing explicit or implicit casts between TIMETZ types with different precision requirements, such as when inserting a high-precision TIMETZ value into a column with lower precision.

The function creates a new TIMETZ value by copying both the time and timezone components from the input, then applies precision adjustment only to the time component using `AdjustTimeForTypmod`. The timezone offset remains unchanged since it doesn't have fractional components that need scaling.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: The input TIMETZ value to be scaled
  - Argument 1: The target type modifier (precision specification)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Extracts the input TIMETZ value from function args
  - PG_GETARG_INT32: Extracts the type modifier from function args
  - [palloc](../p/palloc.md): Allocates memory for the result TIMETZ structure
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md): Applies precision scaling to the time component
  - PG_RETURN_TIMETZADT_P: Returns the scaled TIMETZ value
- Called from (representative examples):
  - PostgreSQL type coercion functions
  - CAST operations in SQL queries
  - Column value assignments with precision constraints

## Notes and Other Information
- This function is registered in the PostgreSQL type system for handling TIMETZ precision coercion
- Only the time component is scaled - the timezone offset is copied unchanged
- Precision scaling follows standard rounding rules (rounds to nearest, ties to even)
- The function always allocates a new TIMETZ structure rather than modifying the input
- Used internally during INSERT, UPDATE, and CAST operations when precision adjustment is needed
- Part of PostgreSQL's comprehensive type coercion system