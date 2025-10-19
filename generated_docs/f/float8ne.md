# float8ne

## Location
[src/backend/utils/adt/float.c:922-930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L922-L930)

## Overview
The float8ne function implements the not-equal comparison operator for PostgreSQL double-precision floating-point numbers, handling special cases like NaN values according to IEEE 754 standards.

## Definition
```c
Datum float8ne(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable wrapper that performs not-equal comparison between two float8 (double-precision) values. It extracts two float8 arguments from the function call context and delegates the actual comparison logic to the inline helper function `float8_ne`. The function properly handles NaN (Not-a-Number) values according to IEEE 754 semantics, where any comparison involving NaN should return true for inequality except when both operands are NaN.

The underlying comparison logic in `float8_ne` implements the rule that:
- If the first value is NaN, the result is true unless the second value is also NaN
- If the first value is not NaN but the second is NaN, the result is true
- Otherwise, it performs a standard floating-point inequality comparison

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - First argument (index 0): float8 value to compare
  - Second argument (index 1): float8 value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 arguments)
  - [float8_ne](float8_ne.md) (inline helper function for the actual comparison)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - SQL queries using the <> or != operators with float8 operands

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:922-930
- This function is typically invoked through PostgreSQLs operator system rather than direct calls
- The NaN handling ensures IEEE 754 compliance where NaN != NaN is false, but NaN != any_other_value is true
- Part of PostgreSQLs comprehensive floating-point arithmetic system

## Simplified Source

```c
Datum float8ne(PG_FUNCTION_ARGS) {
    // Extract the two float8 arguments from SQL call
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 arg2 = PG_GETARG_FLOAT8(1);

    // Delegate to helper function and return boolean result
    return PG_RETURN_BOOL(float8_ne(arg1, arg2));
}
```