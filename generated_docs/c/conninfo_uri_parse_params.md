# conninfo_uri_parse_params

## Location
[src/interfaces/libpq/fe-connect.c:6616-6748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6616-L6748)

## Overview
Parses query parameters from a PostgreSQL connection URI and stores them as connection options with proper URL decoding and validation.

## Definition


## Detailed Description
This function processes the query parameter portion of a PostgreSQL connection URI (the part after the '?' character). It handles key-value pairs separated by '&' characters and performs the following operations:

1. Parses parameter syntax: key=value&key2=value2...
2. Validates proper key-value separator usage (exactly one '=' per parameter)
3. URL-decodes both keys and values using conninfo_uri_decode
4. Handles special keyword compatibility (converts ssl=true to sslmode=require for JDBC compatibility)
5. Stores valid parameters in the connection options array
6. Provides detailed error reporting for malformed parameters

The function destructively modifies the input params buffer during parsing for efficiency.

## Parameters / Member Variables
- : Query parameter string to parse (will be modified during parsing)
- : Array of PQconninfoOption structures to store parsed parameters
- : Buffer to store error messages if parsing fails

## Dependencies
- Functions called/Symbols referenced:
  - [conninfo_uri_decode](conninfo_uri_decode.md)
  - [conninfo_storeval](conninfo_storeval.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - strcmp
  - free
- Called from (representative examples):
  - [conninfo_uri_parse_options](conninfo_uri_parse_options.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns true on successful parsing, false on error
- Performs memory management for decoded strings (malloc/free)
- Includes JDBC compatibility layer (ssl=true → sslmode=require)
- Ignores unknown parameters rather than failing
- Validates parameter syntax strictly (requires exactly one '=' per parameter)
- Handles both known and unknown connection parameters gracefully
- Uses efficient in-place string modification to minimize memory allocation