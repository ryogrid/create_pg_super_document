# int2vectorout

## Location
[src/backend/utils/adt/int.c:207-230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L207-L230)

## Overview
Converts the internal PostgreSQL int2vector data type into its string representation format as space-separated smallint values.

## Definition
```c
Datum int2vectorout(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs the reverse operation of int2vectorin, converting an internal int2vector structure into a human-readable string format. It iterates through all elements in the vector, converting each smallint value to its string representation and separating them with spaces. The function pre-allocates memory based on the assumption that each number requires at most 7 characters (sign + 5 digits + space), ensuring efficient memory usage.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `int2Array`: Pointer to the input int2vector structure to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [int2vector](int2vector.md) (data type)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pg_itoa](../p/pg_itoa.md) (integer to ASCII conversion)
  - `PG_RETURN_CSTRING` (return C-string macro)
- Called from (representative examples):
  - PostgreSQL type input/output system
  - SQL result formatting and display functions

## Notes and Other Information
- Memory allocation assumes maximum 7 characters per number (sign + 5 digits + space)
- Uses pg_itoa for efficient integer-to-string conversion
- Elements are separated by single space characters
- The resulting string is null-terminated
- No error checking is needed as the input is assumed to be a valid int2vector