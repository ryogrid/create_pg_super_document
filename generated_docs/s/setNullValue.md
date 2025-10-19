# setNullValue

## Location
[src/bin/pgbench/pgbench.c:2094-2101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2094-L2101)

## Overview
A utility function in pgbench that initializes a PgBenchValue to represent a NULL value with proper type assignment and memory cleanup.

## Definition
```c
static void setNullValue(PgBenchValue *pv)
```

## Detailed Description
The setNullValue function sets a PgBenchValue to represent a NULL value in pgbench expressions. It performs two key operations:
1. Sets the type field to PGBT_NULL to indicate the value represents NULL
2. Zeroes out the union member (ival) to ensure clean memory state

This function provides a consistent way to create NULL values throughout the pgbench expression evaluation system, ensuring that NULL values are properly represented and can be reliably detected by other functions.

## Parameters / Member Variables
- `pv`: Pointer to PgBenchValue structure to be set to NULL

## Dependencies
- Functions called/Symbols referenced:
  - PGBT_NULL (enum constant for NULL value type)
- Called from (representative examples):
  - [makeVariableValue](../m/makeVariableValue.md) (when creating variable values)
  - [evalLazyFunc](../e/evalLazyFunc.md) (for conditional expressions that may result in NULL)
  - [evalStandardFunc](../e/evalStandardFunc.md) (for functions that may return NULL)

## Notes and Other Information
- This is a static function within pgbench.c, used internally for expression evaluation
- The function zeros the ival member of the union, which is sufficient to clear the entire union
- Used in conditional expressions and error cases where NULL is the appropriate result
- Part of the value management system in pgbench alongside setBoolValue, setIntValue, etc.
- Critical for proper NULL handling in pgbench expressions and SQL-like semantics

## Simplified Source

```c
static void
setNullValue(PgBenchValue *pv)
{
    // Set type to NULL and clear value
    pv->type = PGBT_NULL;
    pv->u.ival = 0;
}
```