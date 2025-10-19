# float4eq

## Location
[src/backend/utils/adt/float.c:819-827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L819-L827)

## Overview
PostgreSQL function that performs equality comparison between two single-precision floating-point numbers (float4) and returns the result as a boolean Datum for use in SQL operations.

## Definition
```c
Datum float4eq(PG_FUNCTION_ARGS)
```

## Detailed Description
float4eq is a PostgreSQL built-in function wrapper that implements the equality operator (=) for single-precision floating-point numbers in SQL. It extracts two float4 arguments from the function call arguments, performs equality comparison using the inline helper function float4_eq(), and returns the boolean result wrapped in a Datum. The function handles NaN values according to PostgreSQL's convention where all NaNs are considered equal to each other, which differs from standard IEEE 754 behavior where NaN != NaN.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention macro that provides access to function arguments and context
  - arg1 (float4): First operand for equality comparison
  - arg2 (float4): Second operand for equality comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4: Macro to extract float4 arguments from function call
  - [float4_eq](float4_eq.md): Inline helper function that performs the actual equality comparison with NaN handling
  - PG_RETURN_BOOL: Macro to return boolean result as Datum
- Called from (representative examples):
  - No direct references found (likely called through SQL operator dispatch)

## Notes and Other Information
- This function serves as the SQL-callable wrapper for the equality operator between float4 values
- The actual comparison is delegated to float4_eq() which implements NaN-aware equality:
  - If both values are NaN, they are considered equal (returns true)
  - If one is NaN and the other is not, they are not equal (returns false)
  - For non-NaN values, standard floating-point equality comparison is used
- This NaN handling ensures consistent sort order and is somewhat arbitrary but necessary for database operations
- Part of PostgreSQL's type system for single-precision floating-point arithmetic
- Located in src/backend/utils/adt/float.c:819-827

## Simplified Source

```c
Datum
float4eq(PG_FUNCTION_ARGS)
{
    // Extract two float4 arguments from function call
    float4 arg1 = PG_GETARG_FLOAT4(0);
    float4 arg2 = PG_GETARG_FLOAT4(1);

    // Perform NaN-aware equality comparison
    // NaN == NaN returns true (PostgreSQL convention)
    // NaN == non-NaN returns false
    // For non-NaN values, use standard equality
    bool result;
    if (isnan(arg1)) {
        result = isnan(arg2);  // Both NaN or only arg1 is NaN
    } else {
        result = !isnan(arg2) && arg1 == arg2;  // Standard equality if neither is NaN
    }

    PG_RETURN_BOOL(result);
}
```