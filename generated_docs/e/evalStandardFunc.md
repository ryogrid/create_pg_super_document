# evalStandardFunc

## Location
[src/bin/pgbench/pgbench.c:2249-2820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2249-L2820)

## Overview
A comprehensive function evaluation engine that handles eager evaluation of all standard pgbench functions, including arithmetic, logical, comparison, mathematical, random, and utility operations.

## Definition
```c
static bool evalStandardFunc(CState *st, PgBenchFunction func, PgBenchExprLink *args, PgBenchValue *retval)
```

## Detailed Description
The `evalStandardFunc` function implements eager evaluation for all non-lazy pgbench functions. It first evaluates all function arguments into a local array, then dispatches to appropriate handlers based on the function type. The function supports extensive type coercion between integers, doubles, and booleans, with proper overflow checking for integer arithmetic. It handles overloaded operators that work on both integer and floating-point types, mathematical functions, random number generation with various distributions, hashing functions, and utility operations like debugging and type casting.

## Parameters / Member Variables
- `st`: Pointer to the current client state (`CState`) containing execution context and random state
- `func`: The `PgBenchFunction` enum value specifying which function to evaluate
- `args`: Linked list of expression arguments (`PgBenchExprLink`) to be eagerly evaluated
- `retval`: Pointer to `PgBenchValue` where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `[evaluateExpr](evaluateExpr.md)` (recursive expression evaluation)
  - `[coerceToInt](../c/coerceToInt.md)`, `coerceToDouble`, `coerceToBool` (type conversions)
  - `[setIntValue](../s/setIntValue.md)`, `setDoubleValue`, `setBoolValue`, `setNullValue` (result setters)
  - Mathematical functions: `sqrt`, `log`, `exp`, `pow`
  - Random generators: `getrand`, `getGaussianRand`, `getZipfianRand`, `getExponentialRand`
  - [Hash](../H/Hash.md) functions: `getHashMurmur2`, `getHashFnv1a`
  - `[permute](../p/permute.md)` (permutation function)
  - Overflow-safe arithmetic: `pg_add_s64_overflow`, `pg_sub_s64_overflow`, `pg_mul_s64_overflow`
  - Various `PGBENCH_*` enum constants for function types
- Called from (representative examples):
  - `[evalFunc](evalFunc.md)`

## Notes and Other Information
- This is a static function with internal linkage, only accessible within pgbench.c
- Handles comprehensive NULL propagation - most functions return NULL if any argument is NULL (except IS and DEBUG)
- Implements proper SQL-like semantics for comparison and arithmetic operations
- Supports type promotion where operations involving doubles return doubles
- Includes extensive error checking for division by zero, parameter ranges, and integer overflow
- The function uses a local array `vargs[MAX_FARGS]` to store evaluated arguments before processing
- Handles variable-argument functions like LEAST/GREATEST that can accept multiple parameters
- Critical component of pgbench's expression evaluation system, handling the majority of built-in functions