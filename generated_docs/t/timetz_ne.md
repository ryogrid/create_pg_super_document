# timetz_ne

## Location
[src/backend/utils/adt/date.c:2479-2487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2479-L2487)

## Overview
A PostgreSQL function that tests for inequality between two time with timezone values, serving as the implementation for the <> or != operator for the timetz data type.

## Definition
```c
Datum timetz_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inequality (not equal) comparison operator for PostgreSQL's time with timezone data type. It extracts two TimeTzADT arguments from the function call and delegates the actual comparison logic to timetz_cmp_internal(). The function returns true if the comparison result is not 0 (indicating inequality), false otherwise.

As a PostgreSQL function following the fmgr interface, it:
- Takes arguments through the PG_FUNCTION_ARGS mechanism
- Returns a Datum containing a boolean result
- Uses PostgreSQL's standard argument extraction macros

The inequality test considers both the time component and timezone component, so two timetz values are considered unequal if either their time values differ or their timezone values differ (or both).

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
  - Database queries using the <> or != operator with timetz values
  - SQL expressions comparing time with timezone values for inequality

## Notes and Other Information
- This function serves as the backend implementation for the SQL <> and != operators for timetz
- Returns true if either time or timezone components differ between the two values
- Part of PostgreSQL's operator function framework for the timetz data type
- The function signature follows PostgreSQL's standard function calling convention
- Logically opposite of timetz_eq function

## Simplified Source

```c
Datum timetz_ne(PG_FUNCTION_ARGS) {
    TimeTzADT *time1 = PG_GETARG_TIMETZADT_P(0);
    TimeTzADT *time2 = PG_GETARG_TIMETZADT_P(1);

    // Compare two timetz values for inequality
    PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) != 0);
}
```