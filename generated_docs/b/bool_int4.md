# bool_int4

## Location
src/backend/utils/adt/int.c: 372 - 395

## Overview
Converts a boolean value to a 32-bit integer (int4), implementing the reverse conversion of int4_bool.

## Definition
```c
Datum bool_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the type conversion from PostgreSQL's boolean data type to int4 (32-bit integer) data type. The conversion follows standard semantics where false converts to 0 and true converts to 1. This function is used internally by PostgreSQL's type system when explicit or implicit casting from boolean to integer is required, particularly in JSON coercion operations and other contexts where boolean values need to be represented as integers.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments through the function call context
  - Argument 0: boolean value to be converted (accessed via `PG_GETARG_BOOL(0)`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL`: Macro to extract boolean argument from function arguments
  - `PG_RETURN_INT32`: Macro to return int32 value from PostgreSQL function
- Called from (representative examples):
  - [ExecEvalJsonCoercion](../E/ExecEvalJsonCoercion.md): Used in JSON coercion operations in expression evaluation

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:372-395`
- This is a standard PostgreSQL V1 calling convention function
- The conversion is straightforward: false → 0, true → 1
- Used for explicit casts like `SELECT true::int` or implicit casts in integer contexts
- Particularly important for JSON operations where boolean values may need integer representation