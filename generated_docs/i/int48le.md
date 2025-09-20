# int48le

## Location
[src/backend/utils/adt/int8.c:263-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L263-L271)

## Overview
A PostgreSQL function that compares a 32-bit integer with a 64-bit integer to determine if the first value is less than or equal to the second value.

## Definition

```c
Datum
int48le(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the "<=" (less than or equal) comparison operator between an  (32-bit integer) and an  (64-bit integer) in PostgreSQL's type system. It extracts two arguments from the function call context - a 32-bit integer as the first argument and a 64-bit integer as the second argument, then performs the comparison and returns a boolean result.

This function is part of PostgreSQL's cross-type comparison operators that allow seamless comparison between different integer types without explicit casting.

## Parameters / Member Variables
- First argument (implicit):  - The 32-bit integer value to be compared
- Second argument (implicit):  - The 64-bit integer value to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts the first 32-bit integer argument
  -  - Extracts the second 64-bit integer argument
  -  - Returns the boolean result of the comparison
- Called from (representative examples):
  - Used through PostgreSQL's operator system when comparing int4 and int8 values

## Notes and Other Information
- Located in 
- This function enables SQL queries like 
- The comparison is safe as the 32-bit value is implicitly promoted to 64-bit for comparison
- Part of a family of cross-type integer comparison functions (int48eq, int48ne, int48lt, int48gt, int48ge)