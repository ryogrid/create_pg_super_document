# i8tod

## Location
[src/backend/utils/adt/int8.c:1283-1296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1283-L1296)

## Overview
Converts a PostgreSQL int8 (64-bit integer) value to a double-precision floating-point number (float8).

## Definition
```c
Datum i8tod(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a type conversion from PostgreSQL's internal 64-bit integer type (int8) to double-precision floating-point (float8). It extracts the int64 argument using PostgreSQL's function argument framework, performs an implicit C type conversion from int64 to double, and returns the result as a PostgreSQL float8 value. The conversion leverages C's built-in type promotion rules.

## Parameters / Member Variables
- The function uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access arguments
- Argument 0: An int8 (64-bit integer) value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro to extract int64 argument)
  - PG_RETURN_FLOAT8 (macro to return float8 result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c:1283-1296
- This is a PostgreSQL built-in function that can be invoked from SQL
- The conversion may lose precision for very large integer values due to the limitations of double-precision floating-point representation
- Part of PostgreSQL's type system for automatic and explicit type conversions between numeric types