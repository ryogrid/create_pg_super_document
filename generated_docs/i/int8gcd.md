# int8gcd

## Location
[src/backend/utils/adt/int8.c:667-681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L667-L681)

## Overview
Public PostgreSQL function wrapper that computes the Greatest Common Divisor (GCD) of two 64-bit signed integers (bigint).

## Definition

```c
Datum
int8gcd(PG_FUNCTION_ARGS)
```
## Detailed Description
The int8gcd function serves as the PostgreSQL-callable wrapper for the GCD operation on bigint values. It follows PostgreSQL's standard function calling convention by extracting two int64 arguments from the function arguments, delegating the actual computation to the internal int8gcd_internal function, and returning the result as a Datum. This design separates the PostgreSQL function interface from the core algorithm implementation, allowing the internal function to be reused by other operations like LCM.

## Parameters / Member Variables
- : First operand extracted as int64
- : Second operand extracted as int64  
- : Stores the GCD result from int8gcd_internal

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extracts int64 arguments)
  - [int8gcd_internal](int8gcd_internal.md) (performs actual GCD computation)
  - PG_RETURN_INT64 (returns int64 result)
- Called from:
  - No direct references found (likely called via PostgreSQL function dispatch system)

## Notes and Other Information
- Simple wrapper function that delegates to int8gcd_internal for implementation
- Follows PostgreSQL function calling conventions with PG_FUNCTION_ARGS and Datum return
- Part of PostgreSQL's comprehensive mathematical function library for bigint operations
- Enables SQL access to GCD functionality through standard PostgreSQL function dispatch
- All complex edge case handling is performed in the internal implementation