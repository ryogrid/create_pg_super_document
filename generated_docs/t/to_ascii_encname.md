# to_ascii_encname

## Location
[src/backend/utils/adt/ascii.c:119-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ascii.c#L119-L137)

## Overview
A PostgreSQL function that converts text to ASCII encoding where the source encoding is specified by name as a string parameter.

## Definition
```c
Datum to_ascii_encname(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that converts text data to ASCII format. It takes two arguments: the text to convert and the encoding name as a string. The function first validates that the provided encoding name is valid using pg_char_to_encoding, and if invalid, raises an error with ERRCODE_UNDEFINED_OBJECT. Upon successful validation, it delegates the actual conversion work to encode_to_ascii.

The function follows PostgreSQL convention for SQL-callable functions by using PG_FUNCTION_ARGS and returning a Datum. It creates a copy of the input text using PG_GETARG_TEXT_P_COPY to ensure safe manipulation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Text data to be converted to ASCII (accessed via PG_GETARG_TEXT_P_COPY)
- `PG_FUNCTION_ARGS[1]`: Name of the source encoding as a PostgreSQL Name type (accessed via PG_GETARG_NAME)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_P_COPY (macro to get text argument with copy)
  - PG_GETARG_NAME (macro to get Name argument)
  - [pg_char_to_encoding](../p/pg_char_to_encoding.md) (converts encoding name to encoding ID)
  - [encode_to_ascii](../e/encode_to_ascii.md) (performs the actual ASCII conversion)
  - PG_RETURN_TEXT_P (macro to return text result)
  - ereport (error reporting function)
- Called from:
  - SQL functions/queries (externally callable)

## Notes and Other Information
- This is a SQL-callable PostgreSQL function exposed to users
- Validates encoding names and provides meaningful error messages for invalid encodings
- Uses a copy of the input text for safe processing
- Error handling follows PostgreSQL standards with appropriate error codes
- The function name suggests it handles encoding conversion by name rather than by ID