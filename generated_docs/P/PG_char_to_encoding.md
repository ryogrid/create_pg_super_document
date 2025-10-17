# PG_char_to_encoding

## Location
[src/backend/utils/mb/mbutils.c:1285-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1285-L1292)

## Overview
A PostgreSQL SQL function wrapper that converts a character encoding name to its corresponding encoding ID number.

## Definition
```c
Datum PG_char_to_encoding(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function interface to the underlying pg_char_to_encoding() function. It takes a PostgreSQL name type parameter containing an encoding name and returns the corresponding encoding ID as an integer. The function performs a binary search through the pg_encname_tbl table to find the matching encoding name after cleaning and normalizing it.

The underlying pg_char_to_encoding() function implements a binary search algorithm through a sorted table of encoding names, returning -1 if the encoding name is not recognized or if the input is invalid.

## Parameters / Member Variables
- Input parameter: PostgreSQL name type containing the encoding name to look up
- Return value: int4 (32-bit integer) representing the encoding ID, or -1 if not found

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME - extracts name argument from function arguments
  - NameStr - converts PostgreSQL name to C string
  - [pg_char_to_encoding](../p/pg_char_to_encoding.md) - performs the actual encoding name lookup
  - PG_RETURN_INT32 - returns integer result to PostgreSQL

- Called from (representative examples):
  - SQL queries via PostgreSQL function call mechanism
  - No direct C code references found

## Notes and Other Information
- The function is registered in the PostgreSQL system catalog as 'pg_char_to_encoding'
- Marked as 'stable' (provolatile => 's') in the system catalog
- The underlying pg_char_to_encoding function is located in src/common/encnames.c:549-586
- Performs binary search through pg_encname_tbl for efficiency
- Input encoding names are cleaned and normalized before comparison
- Function signature location: src/backend/utils/mb/mbutils.c:1285-1292
- Catalog definition: src/include/catalog/pg_proc.dat:3775-3776

## Simplified Source

```c
Datum PG_char_to_encoding(PG_FUNCTION_ARGS) {
    // Extract encoding name from PostgreSQL NAME argument
    Name encoding_name = PG_GETARG_NAME(0);

    // Convert name to encoding ID and return as integer
    PG_RETURN_INT32(pg_char_to_encoding(NameStr(*encoding_name)));
}
```