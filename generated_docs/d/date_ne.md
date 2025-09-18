# date_ne

## Location
src/backend/utils/adt/date.c: 392 - 400

## Overview
date_ne is a comparison function that tests whether two date values are not equal, implementing the inequality operator (<> or !=) for the DATE data type.

## Definition
```c
Datum date_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inequality comparison operator for PostgreSQL's DATE data type. It takes two date arguments through the PostgreSQL function call interface and performs a simple inequality comparison of their internal representations. The function follows PostgreSQL's standard function interface pattern using PG_FUNCTION_ARGS and returns a boolean result indicating whether the two dates are not equal. This is the complement of the date_eq function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument interface that provides:
  - Argument 0: First DateADT value to compare
  - Argument 1: Second DateADT value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT (macro for extracting DateADT arguments)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Types used:
  - DateADT
  - Datum
- Called from (representative examples):
  - Used internally by PostgreSQL's operator system for DATE <> DATE or DATE != DATE operations

## Notes and Other Information
- Part of the comparison function family for dates (along with date_eq, date_lt, etc.)
- Performs direct integer inequality comparison since DateADT is internally represented as days since epoch
- Used by PostgreSQL's operator system to implement the '<>' and '!=' operators for DATE data types
- Returns SQL boolean true if dates are not equal, false if they are equal
- Function follows PostgreSQL's V1 calling convention
- Complement function to date_eq