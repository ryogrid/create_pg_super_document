# bpcharlen

## Location
src/backend/utils/adt/varchar.c: 693 - 708

## Overview
Returns the character length of a CHAR (BpChar) value, excluding trailing spaces and properly handling multibyte character encodings.

## Definition
```c
Datum bpcharlen(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the length calculation for PostgreSQL's CHAR data type (represented internally as BpChar). It first determines the "true" byte length by excluding trailing spaces using `bcTruelen`, then converts this byte length to character length if the database uses a multibyte encoding. This ensures that the returned length represents the actual number of characters rather than bytes, which is important for proper character semantics in multibyte environments. The function handles both single-byte and multibyte character encodings correctly.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention macro that provides access to function arguments, expecting one BpChar argument

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (macro to extract BpChar argument with potential decompression)
  - bcTruelen (function to calculate true length excluding trailing spaces)
  - pg_database_encoding_max_length (function to check if encoding is multibyte)
  - pg_mbstrlen_with_len (function to convert byte length to character length for multibyte strings)
  - VARDATA_ANY (macro to extract data portion from variable-length structure)
  - PG_RETURN_INT32 (macro to return int32 result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's function dispatch system for CHAR_LENGTH() or LENGTH() functions)

## Notes and Other Information
- This function is registered in the PostgreSQL system catalogs as the implementation for CHAR_LENGTH() and LENGTH() functions on CHAR types
- Properly handles multibyte character encodings by converting byte lengths to character counts when necessary
- The function automatically handles detoasting (decompression) of compressed CHAR values through PG_GETARG_BPCHAR_PP
- Trailing spaces are excluded from the length calculation, which is consistent with SQL standard CHAR semantics
- Performance is optimized by only calling multibyte conversion functions when the database encoding requires it