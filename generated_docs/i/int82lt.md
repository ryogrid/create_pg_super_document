# int82lt

## Location
[src/backend/utils/adt/int8.c:302-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L302-L310)

## Overview
A PostgreSQL function that compares a 64-bit integer with a 16-bit integer to determine if the first value is less than the second value.

## Definition
```c
Datum int82lt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int82lt` function implements the "<" (less than) comparison operator between an `int8` (64-bit integer) and an `int2` (16-bit integer) in PostgreSQL's type system. It extracts two arguments from the function call context - a 64-bit integer as the first argument and a 16-bit integer as the second argument, then performs the less-than comparison and returns a boolean result.

This function is part of PostgreSQL's cross-type comparison operators that allow seamless comparison between different integer types without explicit casting. The function is specifically designed to handle comparisons where a 64-bit value is compared against a 16-bit value.

## Parameters / Member Variables
- First argument (implicit): `int64` - The 64-bit integer value to be compared
- Second argument (implicit): `int16` - The 16-bit integer value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64` - Extracts the first 64-bit integer argument
  - `PG_GETARG_INT16` - Extracts the second 16-bit integer argument
  - `PG_RETURN_BOOL` - Returns the boolean result of the comparison
- Called from (representative examples):
  - Used through PostgreSQL's operator system when comparing int8 and int2 values

## Notes and Other Information
- Located in `src/backend/utils/adt/int8.c:302-310`
- This function enables SQL queries like `SELECT -1::bigint < 32767::smallint`
- The comparison is safe as the 16-bit value is implicitly promoted to 64-bit for comparison
- Part of a family of cross-type integer comparison functions (int82eq, int82ne, int82le, int82gt, int82ge)
- Used when a bigint value needs to be compared with a smallint value using the less-than operator