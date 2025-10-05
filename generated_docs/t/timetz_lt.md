# timetz_lt

## Location
[src/backend/utils/adt/date.c:2488-2496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2488-L2496)

## Overview
A PostgreSQL function that tests whether the first time with timezone value is less than the second, serving as the implementation for the < operator for the timetz data type.

## Definition
```c
Datum timetz_lt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than comparison operator for PostgreSQL's time with timezone data type. It extracts two TimeTzADT arguments from the function call and delegates the actual comparison logic to timetz_cmp_internal(). The function returns true if the comparison result is less than 0 (indicating the first argument is less than the second), false otherwise.

The comparison follows the semantics defined in timetz_cmp_internal:
- Primary ordering by GMT-equivalent time (time + timezone offset)
- Secondary ordering by timezone offset if GMT times are equal

This ensures a consistent total ordering of timetz values where the actual instant in time takes precedence, but timezone information provides a secondary sort key.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS:
  - Argument 0: First TimeTzADT value (left operand of < operator)
  - Argument 1: Second TimeTzADT value (right operand of < operator)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P (macro for extracting TimeTzADT arguments)
  - [timetz_cmp_internal](timetz_cmp_internal.md) (internal comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - TimeTzADT (data type)
- Called from (representative examples):
  - Database queries using the < operator with timetz values
  - SQL expressions for ordering time with timezone values
  - Index operations and sorting algorithms

## Notes and Other Information
- This function serves as the backend implementation for the SQL < operator for timetz
- Used by PostgreSQL's query planner for optimization decisions involving timetz comparisons
- Part of PostgreSQL's operator function framework for the timetz data type
- The function signature follows PostgreSQL's standard function calling convention
- Essential for sorting, indexing, and range operations on timetz columns

## Simplified Source

```c
Datum timetz_lt(PG_FUNCTION_ARGS) {
    TimeTzADT *time1 = PG_GETARG_TIMETZADT_P(0);
    TimeTzADT *time2 = PG_GETARG_TIMETZADT_P(1);

    // Compare if first timetz is less than second
    PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) < 0);
}
```