# date_lt

## Location
src/backend/utils/adt/date.c: 401 - 409

## Overview
date_lt is a comparison function that tests whether the first date value is less than the second date value, implementing the less-than operator (<) for the DATE data type.

## Definition
```c
Datum date_lt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than comparison operator for PostgreSQL's DATE data type. It takes two date arguments through the PostgreSQL function call interface and performs a simple less-than comparison of their internal representations. Since DateADT values are internally represented as days since epoch, the comparison directly determines chronological ordering. The function follows PostgreSQL's standard function interface pattern and returns a boolean result indicating whether the first date is chronologically earlier than the second date.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument interface that provides:
  - Argument 0: First DateADT value (left operand)
  - Argument 1: Second DateADT value (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATEADT (macro for extracting DateADT arguments)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Types used:
  - DateADT
  - Datum
- Called from (representative examples):
  - Used internally by PostgreSQL's operator system for DATE < DATE operations

## Notes and Other Information
- Part of the comparison function family for dates (along with date_eq, date_ne, date_le, date_gt, date_ge)
- Performs direct integer comparison since DateADT is internally represented as days since epoch
- Used by PostgreSQL's operator system to implement the '<' operator for DATE data types
- Returns SQL boolean true if the first date is earlier than the second, false otherwise
- Function follows PostgreSQL's V1 calling convention
- Enables chronological ordering and sorting of date values
- Foundation for other temporal comparison operations and date range queries