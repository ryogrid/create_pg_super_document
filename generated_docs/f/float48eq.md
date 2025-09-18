# float48eq

## Location
src/backend/utils/adt/float.c: 3864 - 3872

## Overview
Compares a float4 (single precision) value with a float8 (double precision) value for equality, returning a boolean result.

## Definition
```c
Datum float48eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality comparison operator for mixed-precision floating-point arithmetic in PostgreSQL. It takes a float4 (single precision) value as the first argument and a float8 (double precision) value as the second argument. The function promotes the float4 argument to float8 precision and then performs the equality comparison using the internal float8_eq function, which handles floating-point comparison semantics including special cases like NaN values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1`: float4 (single precision) value to compare
  - `arg2`: float8 (double precision) value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4: Extracts float4 argument from function call context
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
  - [float8_eq](float8_eq.md): Performs the actual float8 equality comparison
  - PG_RETURN_BOOL: Returns the boolean result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:3864-3872
- Part of the comparison operators section for float4/float8 operations
- The float4 argument is implicitly cast to float8 before comparison to ensure consistent precision
- Follows IEEE 754 floating-point comparison semantics
- Returns a Datum containing a boolean result (true if equal, false otherwise)
- Part of a family of comparison functions including float48ne, float48lt, float48le, float48gt, float48ge