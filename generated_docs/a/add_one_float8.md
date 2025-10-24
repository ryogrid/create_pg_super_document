# add_one_float8

## Location
[src/tutorial/funcs.c:36-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/funcs.c#L36-L46)

## Overview
A PostgreSQL C function that increments a double-precision floating-point number by 1.0, demonstrating how to handle floating-point arguments in PostgreSQL user-defined functions.

## Definition
```c
Datum add_one_float8(PG_FUNCTION_ARGS)
```

## Detailed Description
The `add_one_float8` function is a PostgreSQL C function that takes a single double-precision floating-point parameter (float8) and returns the value incremented by 1.0. This function is part of the PostgreSQL tutorial examples, showing how to work with floating-point data types in PostgreSQL C functions. The function demonstrates the use of PostgreSQL's FLOAT8 macros that handle the pass-by-reference nature of floating-point values transparently.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL macro that provides access to function arguments and context information

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8`: Macro to extract a double-precision floating-point argument
  - `PG_RETURN_FLOAT8`: Macro to return a double-precision floating-point value
  - `PG_FUNCTION_INFO_V1`: Macro for function metadata (referenced at line 44)
- Called from (representative examples):
  - [add_one](add_one.md): Referenced from the add_one function context

## Notes and Other Information
- Located in `src/tutorial/funcs.c:36-46`
- This is a tutorial example function demonstrating PostgreSQL floating-point handling
- The comment in the source notes that FLOAT8 macros hide the pass-by-reference nature
- Uses standard PostgreSQL macros for floating-point argument handling and return values
- Follows PostgreSQL's version 1 calling convention
- Demonstrates proper handling of PostgreSQL's float8 data type

## Simplified Source

```c
Datum add_one_float8(PG_FUNCTION_ARGS) {
    // Get the floating-point argument (macros hide pass-by-reference nature)
    float8 arg = PG_GETARG_FLOAT8(0);

    // Return the argument plus 1.0
    PG_RETURN_FLOAT8(arg + 1.0);
}
```