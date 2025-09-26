# setIntValue

## Location
[src/bin/pgbench/pgbench.c:2110-2117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2110-L2117)

## Overview
A utility function in pgbench that initializes a PgBenchValue to represent a 64-bit integer value with the specified numeric value.

## Definition
```c
static void setIntValue(PgBenchValue *pv, int64 ival)
```

## Detailed Description
The setIntValue function sets a PgBenchValue to represent an integer value in pgbench expressions. It performs two key operations:
1. Sets the type field to PGBT_INT to indicate the value represents an integer
2. Assigns the provided int64 value to the ival member of the union

This function provides a consistent way to create integer values throughout the pgbench expression evaluation system, ensuring that integer values are properly typed and can be reliably used in mathematical operations and comparisons.

## Parameters / Member Variables
- `pv`: Pointer to PgBenchValue structure to be set to integer
- `ival`: The 64-bit integer value to assign

## Dependencies
- Functions called/Symbols referenced:
  - PGBT_INT (enum constant for integer value type)
- Called from (representative examples):
  - [makeVariableValue](../m/makeVariableValue.md) (when creating integer variable values)
  - [putVariableInt](../p/putVariableInt.md) (when setting variable to integer value)
  - [evalStandardFunc](../e/evalStandardFunc.md) (for arithmetic operations and mathematical functions)

## Notes and Other Information
- This is a static function within pgbench.c, used internally for expression evaluation
- Most frequently used value setter in the pgbench expression system due to prevalence of integer arithmetic
- Used extensively for mathematical operations (+, -, *, /, %, ^) and their results
- Essential for functions that return integer values (abs, mod, gcd, etc.)
- Part of the value management system alongside setNullValue, setBoolValue, etc.
- Handles 64-bit integers to support large numeric values in pgbench expressions
- Critical component of the arithmetic evaluation pipeline in pgbench