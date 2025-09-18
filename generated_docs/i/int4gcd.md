# int4gcd

## Location
[src/backend/utils/adt/int.c:1294-1308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1294-L1308)

## Overview
PostgreSQL SQL-callable function that computes the greatest common divisor (GCD) of two 32-bit integers.

## Definition
```c
Datum int4gcd(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4gcd` function serves as the PostgreSQL SQL function interface for computing the greatest common divisor of two 32-bit integers. It acts as a thin wrapper around the `int4gcd_internal` function, handling the PostgreSQL function calling protocol by extracting arguments and returning results in the appropriate format. This function can be called directly from SQL queries to compute the GCD of integer values.

## Parameters / Member Variables
- First parameter (accessed via `PG_GETARG_INT32(0)`): First 32-bit integer input
- Second parameter (accessed via `PG_GETARG_INT32(1)`): Second 32-bit integer input

## Dependencies
- Functions called/Symbols referenced:
  - [int4gcd_internal](int4gcd_internal.md): Internal GCD implementation that performs the actual computation
  - `PG_GETARG_INT32()`: PostgreSQL macro to extract int32 arguments
  - `PG_RETURN_INT32()`: PostgreSQL macro to return int32 result
- Called from (representative examples):
  - No direct references found in the codebase (typically called from SQL)

## Notes and Other Information
- This function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS`
- Can be invoked from SQL as a built-in function for GCD computation
- All complex logic and edge case handling is delegated to `int4gcd_internal`
- Part of PostgreSQL's integer arithmetic functions in `src/backend/utils/adt/int.c`
- The actual mathematical computation, overflow handling, and optimization are handled by the internal implementation