# numeric_int8

## Location
[src/backend/utils/adt/numeric.c:4551-4559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4551-L4559)

## Overview
SQL-callable function that converts a PostgreSQL numeric value to a 64-bit signed integer (bigint), throwing errors on conversion failure.

## Definition
```c
Datum numeric_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that converts a numeric data type to an int8 (64-bit signed integer, also known as bigint in SQL). It follows PostgreSQL's standard function calling convention and serves as a wrapper around the more flexible numeric_int8_opt_error function. When conversion fails due to special values (NaN, infinity) or range overflow, this function will throw PostgreSQL errors rather than returning error codes. The function extracts the numeric value from the function arguments, performs the conversion, and returns the result as an int64 Datum.

## Parameters / Member Variables
- Function uses PostgreSQL's standard function argument mechanism (PG_FUNCTION_ARGS)
- Extracts: Numeric value from argument 0

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (macro to extract Numeric from function arguments)
  - [numeric_int8_opt_error](numeric_int8_opt_error.md) (internal conversion function with error handling)
  - PG_RETURN_INT64 (macro to return int64 as Datum)
- Called from (representative examples):
  - [numeric_cash](numeric_cash.md) (currency conversion)
  - [pg_size_bytes](../p/pg_size_bytes.md) (database size utilities)
  - [jsonb_int8](../j/jsonb_int8.md) (JSON conversion functions)

## Notes and Other Information
- This is a SQL-callable function that can be invoked from PostgreSQL SQL statements for type casting
- Uses exception-based error handling (passes NULL to numeric_int8_opt_error)
- Part of PostgreSQL's standard type conversion system
- Commonly used in SQL casts like `CAST(numeric_value AS bigint)` or `numeric_value::bigint`
- Will throw errors for NaN, infinity, or values outside the int64 range (-2^63 to 2^63-1)
- Used internally by other PostgreSQL modules for converting numeric values to integers