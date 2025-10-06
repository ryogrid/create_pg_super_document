# namege

## Location
[src/backend/utils/adt/name.c:193-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L193-L201)

## Overview
The  function implements the "greater than or equal to" comparison operation for PostgreSQL's  data type, used primarily for system catalogs.

## Definition

```c
Datum
namege(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs a "greater than or equal to" comparison between two  values using locale-aware collation. It extracts two  arguments from the function call context, delegates the actual comparison to the  function with the current collation setting, and returns true if the first argument is greater than or equal to the second argument according to the collation rules.

The function is designed as a PostgreSQL built-in function that can be called from SQL queries and is part of the operator infrastructure for the  data type.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to function arguments and metadata
  - : First  value (left operand) extracted using 
  - : Second  value (right operand) extracted using 

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract  arguments from function call
  - : Core comparison function for  values with collation support
  - : Macro to get the current collation for the comparison
  - : Macro to return a boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely used through operator infrastructure)

## Notes and Other Information
- This function is part of PostgreSQL's operator system and is typically invoked through the  operator for  data types
- The comparison respects locale-specific collation rules via 
- Located in  at lines 193-201
- Returns a PostgreSQL  containing a boolean value indicating the comparison result

## Simplified Source

```c
Datum
namege(PG_FUNCTION_ARGS)
{
    Name arg1 = PG_GETARG_NAME(0);
    Name arg2 = PG_GETARG_NAME(1);

    // Compare names using collation and return true if arg1 >= arg2
    PG_RETURN_BOOL(namecmp(arg1, arg2, PG_GET_COLLATION()) >= 0);
}
```