# to_ascii_default

## Location
src/backend/utils/adt/ascii.c: 156 - 173

## Overview
A PostgreSQL function that converts text to ASCII encoding using the current database encoding as the source encoding.

## Definition
```c
Datum to_ascii_default(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that converts text data to ASCII format using the database default encoding as the source. Unlike to_ascii_encname and to_ascii_enc which require explicit encoding specification, this function automatically uses the current database encoding obtained through GetDatabaseEncoding(). This provides a convenient interface for ASCII conversion when the source encoding is known to be the database default.

The function follows PostgreSQL convention for SQL-callable functions by using PG_FUNCTION_ARGS and returning a Datum. It creates a copy of the input text using PG_GETARG_TEXT_P_COPY to ensure safe manipulation and delegates the actual conversion to encode_to_ascii.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Text data to be converted to ASCII (accessed via PG_GETARG_TEXT_P_COPY)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_P_COPY (macro to get text argument with copy)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (function to get current database encoding)
  - [encode_to_ascii](../e/encode_to_ascii.md) (performs the actual ASCII conversion)
  - PG_RETURN_TEXT_P (macro to return text result)
- Called from:
  - SQL functions/queries (externally callable)

## Notes and Other Information
- This is a SQL-callable PostgreSQL function exposed to users
- Automatically uses the database default encoding, no encoding parameter needed
- Simplest interface among the three to_ascii functions for common use cases
- Uses a copy of the input text for safe processing
- No validation needed since GetDatabaseEncoding() always returns a valid encoding
- Most convenient when converting from the database default encoding to ASCII