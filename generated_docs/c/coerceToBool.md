# coerceToBool

## Location
[src/bin/pgbench/pgbench.c:2004-2023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2004-L2023)

## Overview
Attempts to convert a PgBenchValue to a boolean value, providing error handling for incompatible types.

## Definition
```c
static bool coerceToBool(PgBenchValue *pval, bool *bval)
```

## Detailed Description
The `coerceToBool` function performs type coercion from a PgBenchValue to a boolean value with strict type checking. Unlike many programming languages that allow implicit conversion of various types to boolean (e.g., 0/non-zero for integers), this function only accepts values that are already of boolean type. If the input value is of type PGBT_BOOLEAN, it extracts the boolean value and returns success. For any other type (NULL, INT, DOUBLE, or NO_VALUE), it logs an error message using valueTypeName for type identification and returns failure.

## Parameters / Member Variables
- `pval`: Pointer to the PgBenchValue structure to be coerced to boolean
- `bval`: Output parameter that receives the boolean value if coercion succeeds

## Dependencies
- Functions called/Symbols referenced:
  - PgBenchValue - Structure type representing a typed value in pgbench
  - PGBT_BOOLEAN - Enumeration value indicating boolean type
  - [valueTypeName](../v/valueTypeName.md) - Returns string representation of the value's type for error reporting
  - pg_log_error - PostgreSQL logging function for error messages
- Called from (representative examples):
  - evalLazyFunc - Uses coerceToBool for logical operations (AND, OR, NOT)
  - evalStandardFunc - Uses coerceToBool for boolean-requiring functions

## Notes and Other Information
- Returns true on successful coercion, false on failure
- On failure, sets *bval to false to prevent uninitialized variable warnings
- Strict type checking policy - no implicit conversions from other types
- Error messages include the actual type name for better debugging
- This function enforces pgbench's type safety in boolean operations
- Part of pgbench's type coercion system alongside coerceToInt and coerceToDouble