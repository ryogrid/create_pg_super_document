# float4um

## Location
[src/backend/utils/adt/float.c:595-604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L595-L604)

## Overview
Implements the unary minus operation for single-precision floating-point numbers (float4) in PostgreSQL.

## Definition
```c
Datum float4um(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float4um` function is a PostgreSQL built-in function that returns the negation of a float4 (single-precision floating-point) argument. It performs the unary minus operation by applying the C language negation operator (-) to the input value. This function is part of the float4 base operations in PostgreSQL's arithmetic system.

## Parameters / Member Variables
- `result`: local float4 variable to store the negated value

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT4` - macro to extract float4 argument from function call
  - `PG_RETURN_FLOAT4` - macro to return float4 result as Datum
  - `float4` - PostgreSQL type for single-precision floating-point numbers
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:595-604
- Part of PostgreSQL's float4 base operations
- Uses simple C negation operator for the actual computation
- Follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS macro
- Returns result using PostgreSQL's Datum system for type-safe value passing
- The function explicitly stores the result in a local variable before returning it