# date_gt

## Location
[src/backend/utils/adt/date.c:419-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L419-L427)

## Overview
Implements the greater-than comparison operator for PostgreSQL DATE values, returning true if the first date is greater than the second date.

## Definition
```c
Datum date_gt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the SQL ">" operator for DATE data types in PostgreSQL. It extracts two DateADT values from the function arguments and performs a simple integer comparison, since DateADT is internally represented as an integer offset from the PostgreSQL epoch (January 1, 2000). The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS and returns a boolean result using PG_RETURN_BOOL.

## Parameters / Member Variables
- Argument 0: First DateADT value to compare
- Argument 1: Second DateADT value to compare

## Dependencies
- Functions called/Symbols referenced:
  - DateADT (PostgreSQL date type)
  - PG_GETARG_DATEADT (macro to extract DateADT from function arguments)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:419-427
- Part of PostgreSQL's date comparison operator family
- Uses simple integer comparison since DateADT is represented as days since 2000-01-01
- Follows PostgreSQL V1 function calling convention

## Simplified Source

```c
Datum date_gt(PG_FUNCTION_ARGS) {
    DateADT dateVal1 = PG_GETARG_DATEADT(0);
    DateADT dateVal2 = PG_GETARG_DATEADT(1);

    PG_RETURN_BOOL(dateVal1 > dateVal2);
}
```