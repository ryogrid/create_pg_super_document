# coerceToDouble

## Location
[src/bin/pgbench/pgbench.c:2073-2093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2073-L2093)

## Overview
A utility function in pgbench that converts a PgBenchValue to a double-precision floating-point number, performing type coercion with error handling.

## Definition
```c
static bool coerceToDouble(PgBenchValue *pval, double *dval)
```

## Detailed Description
The coerceToDouble function converts values stored in the PgBenchValue union type to double-precision floating-point numbers. It handles different source types appropriately:
- For double values (PGBT_DOUBLE), performs direct assignment
- For integer values (PGBT_INT), casts the integer to double (generally safe conversion)
- For boolean or null values, returns an error as they cannot be meaningfully converted to floating-point numbers

Unlike coerceToInt, this function does not need overflow checking when converting from integers to doubles, as the conversion is generally safe within the range of int64 values.

## Parameters / Member Variables
- `pval`: Input PgBenchValue pointer containing the value to be converted
- `dval`: Output pointer to double where the converted floating-point value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_error (error logging function)
  - valueTypeName (returns string name of value type)
- Called from (representative examples):
  - evalStandardFunc (multiple locations for floating-point mathematical operations)

## Notes and Other Information
- This is a static function within pgbench.c, used internally for expression evaluation
- The function follows PostgreSQL's error handling patterns by returning boolean success/failure status
- Conversion from int64 to double is generally safe and does not require overflow checking
- Used in evalStandardFunc for mathematical operations that require floating-point operands
- Complementary function to coerceToInt for numeric type conversions in pgbench expressions