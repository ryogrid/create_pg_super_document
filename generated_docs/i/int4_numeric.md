# int4_numeric

## Location
src/backend/utils/adt/numeric.c: 4405 - 4412

## Overview
PostgreSQL SQL function that converts a 32-bit integer (int4) to Numeric data type.

## Definition
```c
Datum int4_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the SQL-callable interface for converting PostgreSQL's int4 (32-bit integer) data type to the Numeric data type. It follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS and returns a Datum. The function extracts the int4 value from the function arguments, converts it to int64, and then uses the int64_to_numeric function to perform the actual conversion.

This is a thin wrapper function that provides the SQL interface for the int4 to Numeric conversion, making it accessible from SQL queries and other PostgreSQL operations that require automatic type conversion between these data types.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` macro which provides access to:
  - The int4 value to be converted (accessed via `PG_GETARG_INT32(0)`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro to extract int32 from function arguments)
  - int64_to_numeric (performs the actual conversion)
  - PG_RETURN_NUMERIC (macro to return Numeric result)
- Called from (representative examples):
  - executeItemOptUnwrapTarget (in JSON path execution)
  - JsonItemFromDatum (in JSON processing)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:4405-4412
- This function is registered in PostgreSQL's system catalogs to enable automatic casting from int4 to Numeric
- Part of PostgreSQL's comprehensive type conversion system
- Uses int64_to_numeric internally by casting the int32 value to int64, leveraging the existing 64-bit conversion infrastructure
- Accessible from SQL as an implicit or explicit cast operation