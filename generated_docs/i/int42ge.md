# int42ge

## Location
[src/backend/utils/adt/int.c:603-622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L603-L622)

## Overview
A PostgreSQL function that compares a 32-bit integer (int4) with a 16-bit integer (int2) and returns true if the int4 value is greater than or equal to the int2 value.

## Definition

```c
Datum
int42ge(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the greater-than-or-equal-to comparison operator (>=) between int4 and int2 data types in PostgreSQL. It extracts a 32-bit integer from the first argument and a 16-bit integer from the second argument, performs the comparison, and returns a boolean result. The function follows PostgreSQL's function calling convention using the PG_FUNCTION_ARGS macro and returns a Datum type.

## Parameters / Member Variables
- : 32-bit integer (int4) extracted from the first function argument
- : 16-bit integer (int2) extracted from the second function argument

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro to extract int4 argument)
  - PG_GETARG_INT16 (macro to extract int2 argument)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:603-622
- Part of PostgreSQL's integer arithmetic and comparison operators
- Handles cross-type comparison between different integer sizes
- Uses standard PostgreSQL function calling conventions with PG_FUNCTION_ARGS
- Returns boolean result indicating whether int4 >= int2