# void_out

## Location
src/backend/utils/adt/pseudotypes.c: 269 - 274

## Overview
An output function for the void pseudotype that converts void values to an empty string representation for display purposes.

## Definition
```c
Datum void_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the output conversion function for PostgreSQL's void pseudotype. When a void value needs to be displayed or converted to a string format, this function provides an empty string as the representation. This is necessary to support SQL queries that select functions returning void, such as "SELECT function_returning_void(...)".

The function uses `pstrdup("")` to create a newly allocated empty C string, which is then returned via the `PG_RETURN_CSTRING` macro. This ensures that the PostgreSQL memory management system properly handles the returned string.

Together with `void_in` and `void_send`, this function completes the I/O interface for the void pseudotype, enabling it to work seamlessly within PostgreSQL's type system infrastructure.

## Parameters / Member Variables
- `fcinfo`: Function call information structure (the void input value is ignored)

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_CSTRING
  - [pstrdup](../p/pstrdup.md) (implicitly called to allocate the empty string)
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:269-274
- Part of a trio of void functions: void_in, void_out, and void_send
- Always returns an empty string regardless of input
- Essential for making "SELECT function_returning_void(...)" work properly in SQL
- Uses PostgreSQL's memory allocation to ensure proper cleanup
- Provides a consistent string representation for void values in query results