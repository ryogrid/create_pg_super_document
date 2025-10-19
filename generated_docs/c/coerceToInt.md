# coerceToInt

## Location
[src/bin/pgbench/pgbench.c:2045-2072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2045-L2072)

## Overview
A utility function in pgbench that converts a PgBenchValue to a 64-bit integer, performing type coercion with overflow checking and error handling.

## Definition

```c
static bool
coerceToInt(PgBenchValue *pval, int64 *ival)
```
## Detailed Description
The coerceToInt function converts values stored in the PgBenchValue union type to 64-bit integers. It handles different source types appropriately:
- For integer values (PGBT_INT), performs direct assignment
- For double precision values (PGBT_DOUBLE), rounds to nearest integer and checks for overflow conditions
- For boolean or null values, returns an error as they cannot be meaningfully converted to integers

The function includes safety checks for NaN values and integer overflow when converting from double to int64. On error conditions, it logs appropriate error messages and returns false to indicate failure.

## Parameters / Member Variables
- `*pval`: Input PgBenchValue pointer containing the value to be converted
- `*ival`: Output pointer to int64 where the converted integer value will be stored
## Dependencies
- Functions called/Symbols referenced:
  - rint (rounds double to nearest integer)
  - isnan (checks for NaN values)
  - FLOAT8_FITS_IN_INT64 (macro to check if double fits in int64)
  - pg_log_error (error logging function)
  - [valueTypeName](../v/valueTypeName.md) (returns string name of value type)
- Called from (representative examples):
  - [evalStandardFunc](../e/evalStandardFunc.md) (multiple locations for various mathematical operations)

## Notes and Other Information
- This is a static function within pgbench.c, used internally for expression evaluation
- The function follows PostgreSQL's error handling patterns by returning boolean success/failure status
- Overflow checking is critical when converting floating-point values to integers to prevent undefined behavior
- Used extensively in evalStandardFunc for mathematical operations that require integer operands

## Simplified Source

```c
static bool coerceToInt(PgBenchValue *pval, int64 *ival) {
    // Direct assignment for integer values
    if (pval->type == PGBT_INT) {
        *ival = pval->u.ival;
        return true;
    }

    // Convert double to int with overflow checking
    if (pval->type == PGBT_DOUBLE) {
        double rounded_value = rint(pval->u.dval);

        // Check for NaN and overflow
        if (isnan(rounded_value) || !FLOAT8_FITS_IN_INT64(rounded_value)) {
            pg_log_error("double to int overflow for %f", rounded_value);
            return false;
        }

        *ival = (int64) rounded_value;
        return true;
    }

    // Cannot convert boolean or null to int
    pg_log_error("cannot coerce %s to int", valueTypeName(pval));
    return false;
}
```