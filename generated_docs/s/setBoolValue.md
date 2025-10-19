# setBoolValue

## Location
[src/bin/pgbench/pgbench.c:2102-2109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2102-L2109)

## Overview
A utility function in pgbench that initializes a PgBenchValue to represent a boolean value with the specified truth value.

## Definition
```c
static void setBoolValue(PgBenchValue *pv, bool bval)
```

## Detailed Description
The setBoolValue function sets a PgBenchValue to represent a boolean value in pgbench expressions. It performs two key operations:
1. Sets the type field to PGBT_BOOLEAN to indicate the value represents a boolean
2. Assigns the provided boolean value to the bval member of the union

This function provides a consistent way to create boolean values throughout the pgbench expression evaluation system, ensuring that boolean values are properly typed and can be reliably used in conditional expressions and logical operations.

## Parameters / Member Variables
- `pv`: Pointer to PgBenchValue structure to be set to boolean
- `bval`: The boolean value (true or false) to assign

## Dependencies
- Functions called/Symbols referenced:
  - PGBT_BOOLEAN (enum constant for boolean value type)
- Called from (representative examples):
  - [makeVariableValue](../m/makeVariableValue.md) (when creating boolean variable values)
  - [evalLazyFunc](../e/evalLazyFunc.md) (for conditional expressions that result in boolean)
  - [evalStandardFunc](../e/evalStandardFunc.md) (for comparison operations and logical functions)

## Notes and Other Information
- This is a static function within pgbench.c, used internally for expression evaluation
- Widely used throughout the expression evaluation system for comparison operations (==, !=, <, >, <=, >=)
- Essential for logical operations and conditional expressions in pgbench scripts
- Part of the value management system alongside setNullValue, setIntValue, etc.
- Used extensively in evalStandardFunc for comparison and logical operations
- Critical for implementing SQL-like boolean semantics in pgbench expressions

## Simplified Source

```c
static void
setBoolValue(PgBenchValue *pv, bool bval)
{
    // Set type to boolean and assign value
    pv->type = PGBT_BOOLEAN;
    pv->u.bval = bval;
}
```