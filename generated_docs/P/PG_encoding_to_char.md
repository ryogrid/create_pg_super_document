# PG_encoding_to_char

## Location
[src/backend/utils/mb/mbutils.c:1293-1307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1293-L1307)

## Overview
A PostgreSQL SQL function wrapper that converts an encoding ID number to its corresponding character encoding name.

## Definition
```c
Datum PG_encoding_to_char(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function interface to the underlying pg_encoding_to_char() function. It takes an integer encoding ID as input and returns the corresponding encoding name as a PostgreSQL name type. The function validates the encoding ID using PG_VALID_ENCODING and looks up the name from the pg_enc2name_tbl table.

The underlying pg_encoding_to_char() function performs a simple array lookup in the pg_enc2name_tbl table, returning an empty string if the encoding ID is invalid.

## Parameters / Member Variables
- Input parameter: int4 (32-bit integer) representing the encoding ID
- Return value: PostgreSQL name type containing the encoding name, or empty string if invalid

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 - extracts integer argument from function arguments
  - pg_encoding_to_char - performs the actual encoding ID to name lookup
  - DirectFunctionCall1 - PostgreSQL function call framework
  - namein - converts C string to PostgreSQL name type
  - [CStringGetDatum](../C/CStringGetDatum.md) - converts C string to PostgreSQL Datum

- Called from (representative examples):
  - SQL queries via PostgreSQL function call mechanism
  - No direct C code references found

## Notes and Other Information
- The function is registered in the PostgreSQL system catalog as 'pg_encoding_to_char'
- Marked as 'stable' (provolatile => 's') in the system catalog
- The underlying pg_encoding_to_char function is located in src/common/encnames.c:587-597
- Uses direct array indexing into pg_enc2name_tbl for O(1) lookup performance
- Returns empty string for invalid encoding IDs rather than NULL
- Function signature location: src/backend/utils/mb/mbutils.c:1293-1307
- Catalog definition: src/include/catalog/pg_proc.dat:3779-3780
- Validates encoding ID using PG_VALID_ENCODING macro before lookup