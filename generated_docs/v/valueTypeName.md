# valueTypeName

## Location
[src/bin/pgbench/pgbench.c:1982-2003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1982-L2003)

## Overview
Returns a human-readable string representation of a PgBenchValue's data type for error reporting and debugging purposes.

## Definition
```c
static char *valueTypeName(PgBenchValue *pval)
```

## Detailed Description
The `valueTypeName` function provides a mapping from PgBenchValue type enumeration values to their corresponding string representations. It examines the type field of a PgBenchValue structure and returns an appropriate descriptive string. This function is primarily used for error reporting and debugging, allowing pgbench to provide meaningful type information in error messages when type coercion or validation fails. The function handles all valid PgBenchValue types and includes an assertion for detecting internal errors if an invalid type is encountered.

## Parameters / Member Variables
- `pval`: Pointer to a PgBenchValue structure whose type field will be examined

## Dependencies
- Functions called/Symbols referenced:
  - PgBenchValue - Structure type representing a value with type information
  - PGBT_NO_VALUE - Enumeration value for uninitialized/empty values
  - PGBT_NULL - Enumeration value for NULL values
  - PGBT_INT - Enumeration value for integer values
  - PGBT_DOUBLE - Enumeration value for floating-point values
  - PGBT_BOOLEAN - Enumeration value for boolean values
- Called from (representative examples):
  - [coerceToBool](../c/coerceToBool.md) - Uses valueTypeName for error reporting when type coercion fails
  - [coerceToInt](../c/coerceToInt.md) - Uses valueTypeName for error reporting when type coercion fails
  - [coerceToDouble](../c/coerceToDouble.md) - Uses valueTypeName for error reporting when type coercion fails

## Notes and Other Information
- Returns static string literals, so the returned pointer should not be freed
- The function includes an Assert(false) for invalid types, indicating this should never occur in normal operation
- Type strings returned are: "none", "null", "int", "double", "boolean"
- This is a utility function that enhances the debugging and error reporting capabilities of pgbench's type system
- The function is designed to be fast and lightweight since it's used in error paths