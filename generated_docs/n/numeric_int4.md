# numeric_int4

## Location
[src/backend/utils/adt/numeric.c:4463-4475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4463-L4475)

## Overview
PostgreSQL SQL function that converts a Numeric value to a 32-bit signed integer (int4).

## Definition
```c
Datum numeric_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the SQL-callable interface for converting PostgreSQL's Numeric data type to int4 (32-bit signed integer). It follows the standard PostgreSQL function calling convention and provides the standard conversion behavior with exception throwing for invalid conversions. The function delegates the actual conversion work to numeric_int4_opt_error with NULL for the error handling parameter, which means any conversion errors will result in PostgreSQL exceptions being thrown.

This is the function that gets called when performing explicit casts from Numeric to int4 in SQL queries, or when PostgreSQL's type system requires automatic conversion between these types.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` macro which provides access to:

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (macro to extract Numeric from function arguments)
  - [numeric_int4_opt_error](numeric_int4_opt_error.md) (performs the actual conversion)
  - PG_RETURN_INT32 (macro to return int32 result)
  - [Numeric](../N/Numeric.md) (data type)
- Called from (representative examples):
  - [numeric_to_char](numeric_to_char.md) (in formatting operations)
  - [jsonb_int4](../j/jsonb_int4.md) (in JSONB processing)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:4463-4475
- This function is registered in PostgreSQL's system catalogs to enable casting from Numeric to int4
- Part of PostgreSQL's comprehensive type conversion system
- Always throws exceptions on conversion errors (uses NULL for have_error parameter in numeric_int4_opt_error)
- Accessible from SQL as an explicit cast: `SELECT numeric_value::int4` or `CAST(numeric_value AS int4)`
- Used in formatting and JSON processing where Numeric values need to be converted to integers