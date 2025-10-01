# date_cmp

## Location
[src/backend/utils/adt/date.c:437-449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L437-L449)

## Overview
Implements the comparison function for PostgreSQL DATE values, returning -1, 0, or 1 to indicate whether the first date is less than, equal to, or greater than the second date.

## Definition
```c
Datum date_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a three-way comparison for DATE data types in PostgreSQL, typically used for sorting and indexing operations. It extracts two DateADT values from the function arguments and compares them using simple integer arithmetic, since DateADT is internally represented as an integer offset from the PostgreSQL epoch (January 1, 2000). The function returns -1 if the first date is earlier, 1 if the first date is later, and 0 if both dates are equal. This follows the standard C library qsort comparison function convention.

## Parameters / Member Variables
- Argument 0: First DateADT value to compare
- Argument 1: Second DateADT value to compare

## Dependencies
- Functions called/Symbols referenced:
  - DateADT (PostgreSQL date type)
  - PG_GETARG_DATEADT (macro to extract DateADT from function arguments)
  - PG_RETURN_INT32 (macro to return 32-bit integer result)
- Called from (representative examples):
  - [compareDatetime](../c/compareDatetime.md) (in src/backend/utils/adt/jsonpath_exec.c:3736)

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:437-449
- Essential for date sorting operations and B-tree indexing
- Returns standard comparison values: -1 (less), 0 (equal), 1 (greater)
- Uses simple integer comparison since DateADT is represented as days since 2000-01-01
- Follows PostgreSQL V1 function calling convention

## Simplified Source

```c
Datum
date_cmp(PG_FUNCTION_ARGS)
{
    // Extract date values from function arguments
    DateADT date1 = PG_GETARG_DATEADT(0);
    DateADT date2 = PG_GETARG_DATEADT(1);

    // Simple integer comparison since DateADT is stored as days since epoch
    if (date1 < date2)
        PG_RETURN_INT32(-1);
    else if (date1 > date2)
        PG_RETURN_INT32(1);

    PG_RETURN_INT32(0);  // dates are equal
}
```