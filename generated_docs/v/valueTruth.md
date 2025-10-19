# valueTruth

## Location
[src/bin/pgbench/pgbench.c:2024-2044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2024-L2044)

## Overview
Evaluates the truthiness of a PgBenchValue for conditional expressions, implementing C-like truthiness rules across all pgbench data types.

## Definition
```c
static bool valueTruth(PgBenchValue *pval)
```

## Detailed Description
The `valueTruth` function provides a unified way to evaluate the boolean truthiness of any PgBenchValue for use in conditional expressions and control flow. Unlike `coerceToBool` which requires strict boolean types, this function implements implicit truthiness conversion following C-like semantics: NULL values are false, boolean values retain their truth value, non-zero numerical values (both integer and double) are true, and zero values are false. This function is essential for implementing conditional logic in pgbench scripts, allowing expressions like \\if statements to work with various data types in an intuitive manner.

## Parameters / Member Variables
- `pval`: Pointer to the PgBenchValue structure to evaluate for truthiness

## Dependencies
- Functions called/Symbols referenced:
  - PgBenchValue - Structure type representing a typed value in pgbench
  - PGBT_NULL - Enumeration value for NULL values (evaluates to false)
  - PGBT_BOOLEAN - Enumeration value for boolean values (uses actual boolean value)
  - PGBT_INT - Enumeration value for integer values (non-zero is true)
  - PGBT_DOUBLE - Enumeration value for double values (non-zero is true)
- Called from (representative examples):
  - [evalLazyFunc](../e/evalLazyFunc.md) - Uses valueTruth for conditional evaluation in logical expressions
  - [executeMetaCommand](../e/executeMetaCommand.md) - Uses valueTruth for \\if conditional statements in pgbench scripts

## Notes and Other Information
- Implements C-like truthiness: 0 and NULL are false, everything else is true
- Handles floating-point comparison with 0.0 for double values
- Includes assertion for unexpected types, indicating internal error if triggered
- More permissive than coerceToBool - allows implicit conversion from numeric types
- Essential for pgbench's conditional execution and control flow features
- Returns false for any unexpected types (with assertion failure in debug builds)
- Designed to be fast since it's used frequently in expression evaluation

## Simplified Source

```c
static bool valueTruth(PgBenchValue *pval) {
    // Evaluate truthiness with C-like semantics: NULL and 0 are false, everything else true
    switch (pval->type) {
        case PGBT_NULL:    return false;                // NULL is always false
        case PGBT_BOOLEAN: return pval->u.bval;         // Use boolean value directly
        case PGBT_INT:     return pval->u.ival != 0;    // Non-zero integers are true
        case PGBT_DOUBLE:  return pval->u.dval != 0.0;  // Non-zero doubles are true
        default:
            Assert(0);  // Unexpected type - internal error
            return false;
    }
}
```