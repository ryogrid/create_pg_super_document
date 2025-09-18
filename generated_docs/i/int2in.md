# int2in

## Location
[src/backend/utils/adt/int.c:63-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L63-L73)

## Overview
The int2in function converts a string representation of a number to a PostgreSQL int2 (16-bit signed integer) data type.

## Definition
```c
Datum int2in(PG_FUNCTION_ARGS)
```

## Detailed Description
int2in is a PostgreSQL input function that serves as the string-to-int2 conversion routine. It is part of the USER I/O ROUTINES for the int2 data type. The function takes a C-style string containing a numeric representation and converts it to PostgreSQL's internal int2 format (16-bit signed integer). The function uses pg_strtoint16_safe for safe conversion, which includes proper error handling and validation of the input string. This function is typically called automatically by PostgreSQL when converting string literals to int2 values during SQL parsing and execution.

## Parameters / Member Variables
- `num`: C-string containing the textual representation of the integer to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strtoint16_safe](../p/pg_strtoint16_safe.md)
  - PG_RETURN_INT16
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's type input/output system
- Uses safe conversion routines to prevent buffer overflows and handle invalid input
- Returns a Datum containing the converted int2 value
- Error handling is delegated to pg_strtoint16_safe which will report appropriate errors for invalid input
- The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS