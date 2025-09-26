# pg_convert

## Location
[src/backend/utils/mb/mbutils.c:553-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L553-L614)

## Overview
The core SQL function for converting bytea data between arbitrary character encodings, providing the foundation for PostgreSQL's encoding conversion system.

## Definition
```c
Datum pg_convert(PG_FUNCTION_ARGS)
```

SQL Function Signature:
```sql
BYTEA convert(BYTEA string, NAME src_encoding_name, NAME dest_encoding_name)
```

## Detailed Description
This function performs character encoding conversion between any two supported encodings. It serves as the primary SQL interface for encoding conversion and is used as the foundation for the more specialized pg_convert_to and pg_convert_from functions.

The function performs comprehensive validation of both the input parameters and the source data:
- Validates that both source and destination encoding names are recognized
- Verifies that the source data is valid in the specified source encoding  
- Delegates the actual conversion to pg_do_encoding_conversion
- Handles memory management for the result, including proper cleanup
- Returns the original input if no conversion is needed (optimization)

Key characteristics:
- Works with bytea types to handle arbitrary byte sequences safely
- Validates encoding names and source data integrity
- Optimizes no-conversion cases by returning the original input
- Manages memory allocation and cleanup for conversion results
- Provides detailed error messages for invalid encoding names

## Parameters / Member Variables
- `string`: Input bytea data to convert (PG_FUNCTION_ARG 0)
- `src_encoding_name`: Name of the source encoding (PG_FUNCTION_ARG 1)
- `dest_encoding_name`: Name of the destination encoding (PG_FUNCTION_ARG 2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP/PG_GETARG_NAME (argument extraction macros)
  - [pg_char_to_encoding](pg_char_to_encoding.md) (encoding name to ID conversion)
  - [pg_verify_mbstr](pg_verify_mbstr.md) (source data validation)
  - [pg_do_encoding_conversion](pg_do_encoding_conversion.md) (core conversion function)
  - unconstify (const removal utility)
  - Memory management functions (palloc, pfree, SET_VARSIZE)
  - PG_RETURN_BYTEA_P/PG_FREE_IF_COPY (return and cleanup macros)
- Called from (representative examples):
  - [pg_convert_to](pg_convert_to.md) (database-to-encoding conversion)
  - [pg_convert_from](pg_convert_from.md) (encoding-to-database conversion)

## Notes and Other Information
- Primary SQL interface for general encoding conversion
- Used as the foundation by pg_convert_to and pg_convert_from wrapper functions
- Performs thorough validation of both encoding names and source data
- Returns bytea to preserve exact byte sequences after conversion
- Optimizes cases where no conversion is needed by returning original input
- Handles toasted input values appropriately with PG_FREE_IF_COPY
- Part of PostgreSQL's comprehensive character set support system
- Available directly to users for arbitrary encoding conversions