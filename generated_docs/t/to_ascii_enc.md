# to_ascii_enc

## Location
[src/backend/utils/adt/ascii.c:138-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ascii.c#L138-L155)

## Overview
A PostgreSQL function that converts text to ASCII encoding where the source encoding is specified by an integer encoding ID.

## Definition
```c
Datum to_ascii_enc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that converts text data to ASCII format, similar to to_ascii_encname but takes an integer encoding ID instead of an encoding name string. The function validates that the provided encoding ID is valid using the PG_VALID_ENCODING macro, and if invalid, raises an error with ERRCODE_UNDEFINED_OBJECT. Upon successful validation, it delegates the actual conversion work to encode_to_ascii.

The function follows PostgreSQL convention for SQL-callable functions by using PG_FUNCTION_ARGS and returning a Datum. It creates a copy of the input text using PG_GETARG_TEXT_P_COPY to ensure safe manipulation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Text data to be converted to ASCII (accessed via PG_GETARG_TEXT_P_COPY)
- `PG_FUNCTION_ARGS[1]`: Integer encoding ID representing the source encoding (accessed via PG_GETARG_INT32)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_P_COPY (macro to get text argument with copy)
  - PG_GETARG_INT32 (macro to get 32-bit integer argument)
  - PG_VALID_ENCODING (macro to validate encoding ID)
  - [encode_to_ascii](../e/encode_to_ascii.md) (performs the actual ASCII conversion)
  - PG_RETURN_TEXT_P (macro to return text result)
  - ereport (error reporting function)
- Called from:
  - SQL functions/queries (externally callable)

## Notes and Other Information
- This is a SQL-callable PostgreSQL function exposed to users
- Validates encoding IDs using PG_VALID_ENCODING macro
- Provides meaningful error messages for invalid encoding codes
- Uses a copy of the input text for safe processing
- Companion function to to_ascii_encname, differing only in parameter type (int vs name)
- Error handling follows PostgreSQL standards with appropriate error codes

## Simplified Source

```c
Datum to_ascii_enc(PG_FUNCTION_ARGS) {
    text *data = PG_GETARG_TEXT_P_COPY(0);
    int enc = PG_GETARG_INT32(1);

    // Validate the encoding ID
    if (!PG_VALID_ENCODING(enc)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("%d is not a valid encoding code", enc)));
    }

    // Convert to ASCII and return result
    PG_RETURN_TEXT_P(encode_to_ascii(data, enc));
}
```

This function converts text to ASCII using an integer encoding ID. It validates the encoding parameter, creates a copy of the input text for safe processing, then delegates the actual conversion to `encode_to_ascii()`. Error handling provides clear messages for invalid encoding codes.