# void_in

## Location
src/backend/utils/adt/pseudotypes.c: 263 - 268

## Overview
An input function for the void pseudotype that accepts any input and returns a void value, primarily used to support PL functions that return VOID.

## Definition
```c
Datum void_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the input conversion function for PostgreSQL's void pseudotype. The void type represents "no value" and is primarily used as a return type for functions that perform actions but don't return data. The `void_in` function is designed to accept any input string and simply ignore it, always returning a void value.

This function exists specifically to support procedural languages (PL functions) that need to return VOID without requiring special handling in the PL handler. Whatever value the PL function thinks it's returning will be ignored, and the function will always produce a proper void result.

The function includes a humorous comment acknowledging that callers shouldn't expect any meaningful processing of the input - it simply discards whatever is provided and returns void.

## Parameters / Member Variables
- `fcinfo`: Function call information structure (input is ignored, any value is discarded)

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:263-268
- Part of a trio of void functions: void_in, void_out, and void_send
- Enables "SELECT function_returning_void(...)" to work properly
- The input parameter is completely ignored - any string input will result in the same void output
- Essential for supporting procedural languages that return void values
- Contains developer humor: "you were expecting something different?" comment