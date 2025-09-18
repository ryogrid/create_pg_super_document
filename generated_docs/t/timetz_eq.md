# timetz_eq

## Location
src/backend/utils/adt/date.c: 2470 - 2478

## Overview
A PostgreSQL function that tests for equality between two time with timezone values, serving as the implementation for the = operator for the timetz data type.

## Definition
```c
Datum timetz_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality comparison operator for PostgreSQL's time with timezone data type. It extracts two TimeTzADT arguments from the function call and delegates the actual comparison logic to timetz_cmp_internal(). The function returns true if the comparison result is 0 (indicating equality), false otherwise.

As a PostgreSQL function following the fmgr interface, it:
- Takes arguments through the PG_FUNCTION_ARGS mechanism
- Returns a Datum containing a boolean result
- Uses PostgreSQL's standard argument extraction macros

The equality test considers both the time component and timezone component, so two timetz values are equal only if they have identical time and timezone values, not just if they represent the same instant.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS:
  - Argument 0: First TimeTzADT value to compare
  - Argument 1: Second TimeTzADT value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P (macro for extracting TimeTzADT arguments)
  - [timetz_cmp_internal](timetz_cmp_internal.md) (internal comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - TimeTzADT (data type)
- Called from (representative examples):
  - Database queries using the = operator with timetz values
  - SQL expressions comparing time with timezone values

## Notes and Other Information
- This function serves as the backend implementation for the SQL = operator for timetz
- Returns true only if both time and timezone components are identical
- Part of PostgreSQL's operator function framework for the timetz data type
- The function signature follows PostgreSQL's standard function calling convention