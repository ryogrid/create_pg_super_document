# timetz_le

## Location
src/backend/utils/adt/date.c: 2497 - 2505

## Overview
A PostgreSQL function that tests whether the first time with timezone value is less than or equal to the second, serving as the implementation for the <= operator for the timetz data type.

## Definition
```c
Datum timetz_le(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than-or-equal-to comparison operator for PostgreSQL's time with timezone data type. It extracts two TimeTzADT arguments from the function call and delegates the actual comparison logic to timetz_cmp_internal(). The function returns true if the comparison result is less than or equal to 0 (indicating the first argument is less than or equal to the second), false otherwise.

The comparison follows the same semantics as other timetz comparison functions:
- Primary ordering by GMT-equivalent time (time + timezone offset)
- Secondary ordering by timezone offset if GMT times are equal
- Returns true for both less-than and equal-to cases

This provides the logical combination of the < and = operators, essential for range queries and ordering operations.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS:
  - Argument 0: First TimeTzADT value (left operand of <= operator)
  - Argument 1: Second TimeTzADT value (right operand of <= operator)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P (macro for extracting TimeTzADT arguments)
  - timetz_cmp_internal (internal comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - TimeTzADT (data type)
- Called from (representative examples):
  - Database queries using the <= operator with timetz values
  - SQL expressions for range queries with time with timezone values
  - BETWEEN clauses and similar range operations

## Notes and Other Information
- This function serves as the backend implementation for the SQL <= operator for timetz
- Combines the logic of both less-than and equal-to comparisons
- Essential for range queries, sorting, and indexing operations on timetz columns
- Part of PostgreSQL's operator function framework for the timetz data type
- The function signature follows PostgreSQL's standard function calling convention
- Used extensively in WHERE clauses, ORDER BY statements, and constraint checking