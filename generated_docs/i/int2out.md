# int2out

## Location
[src/backend/utils/adt/int.c:74-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L74-L86)

## Overview
The int2out function converts a PostgreSQL int2 (16-bit signed integer) value to its string representation.

## Definition
```c
Datum int2out(PG_FUNCTION_ARGS)
```

## Detailed Description
int2out is a PostgreSQL output function that serves as the int2-to-string conversion routine. It is part of the USER I/O ROUTINES for the int2 data type. The function takes an int2 value and converts it to a null-terminated C-string representation. The function allocates memory for the result string (7 bytes to accommodate the sign, up to 5 digits for a 16-bit integer, and the null terminator) and uses pg_itoa for the actual integer-to-string conversion. This function is typically called automatically by PostgreSQL when displaying int2 values in query results or when casting int2 values to text.

## Parameters / Member Variables
- `arg1`: The int2 (16-bit signed integer) value to be converted to string

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16
  - [pg_itoa](../p/pg_itoa.md)
  - PG_RETURN_CSTRING
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's type input/output system
- Allocates exactly 7 bytes for the result string (sign + 5 digits + null terminator)
- Uses pg_itoa for efficient integer-to-string conversion
- Memory is allocated using palloc, which is automatically freed by PostgreSQL's memory context system
- The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- Returns a Datum containing a C-string pointer