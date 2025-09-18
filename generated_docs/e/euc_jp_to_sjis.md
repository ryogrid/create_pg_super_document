# euc_jp_to_sjis

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 61 - 76

## Overview
PostgreSQL encoding conversion function that converts text from EUC-JP (Extended Unix Code for Japanese) encoding to Shift_JIS (SJIS) encoding.

## Definition
```c
Datum euc_jp_to_sjis(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function wrapper for EUC-JP to Shift_JIS character encoding conversion. It extracts parameters from PostgreSQL function arguments, validates the encoding types, performs the actual conversion using the `euc_jp2sjis` helper function, and returns the number of bytes processed. The function handles multi-byte character sequences including ASCII characters, hankaku katakana (half-width katakana), JIS X0208 kanji, JIS X0212 kanji, and user-defined characters (UDC). It also handles IBM-specific kanji characters through lookup tables.

## Parameters / Member Variables
The function uses PostgreSQLs `PG_FUNCTION_ARGS` macro to access arguments:
- `PG_GETARG_CSTRING(2)`: Source string in EUC-JP encoding
- `PG_GETARG_CSTRING(3)`: Destination buffer for SJIS-encoded output
- `PG_GETARG_INT32(4)`: Length of the source string
- `PG_GETARG_BOOL(5)`: noError flag - when true, stops conversion on invalid characters instead of throwing errors

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL`: Extracts boolean argument from PostgreSQL function call
  - `PG_EUC_JP`: Encoding constant for EUC-JP
  - `PG_SJIS`: Encoding constant for Shift_JIS
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validates encoding conversion parameters
  - `[euc_jp2sjis](euc_jp2sjis.md)`: Core conversion function that performs the actual character-by-character conversion
  - `PG_RETURN_INT32`: Returns integer result to PostgreSQL
- Called from (representative examples):
  - `PGEUCALTCODE`: Referenced in the same source file

## Notes and Other Information
- This function is part of PostgreSQLs multi-byte character encoding conversion system
- The actual conversion logic handles complex Japanese character mappings including special cases for user-defined characters and IBM kanji extensions
- The function follows PostgreSQLs V1 calling convention for user-defined functions
- Error handling can be controlled via the noError parameter to allow graceful handling of invalid character sequences
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c at lines 61-76