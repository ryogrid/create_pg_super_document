# sjis_to_mic

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 125 - 140

## Overview
PostgreSQL encoding conversion function that converts text from Shift_JIS (SJIS) encoding to MIC (Mule Internal Code) encoding.

## Definition
```c
Datum sjis_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function wrapper for Shift_JIS to MIC (Mule Internal Code) character encoding conversion. It extracts parameters from PostgreSQL function arguments, validates the encoding types, performs the actual conversion using the `sjis2mic` helper function, and returns the number of bytes processed. The conversion process analyzes Shift_JIS character sequences and adds appropriate leading codes (LC) for MIC encoding: ASCII characters pass through unchanged, JIS X0201 half-width katakana gets LC_JISX0201K prefix, JIS X0208 kanji gets LC_JISX0208 prefix, and JIS X0212 kanji gets LC_JISX0212 prefix. The function handles complex mappings including user-defined character areas (UDC1/UDC2) and IBM kanji variants with lookup table support.

## Parameters / Member Variables
The function uses PostgreSQLs `PG_FUNCTION_ARGS` macro to access arguments:
- `PG_GETARG_CSTRING(2)`: Source string in Shift_JIS encoding
- `PG_GETARG_CSTRING(3)`: Destination buffer for MIC-encoded output
- `PG_GETARG_INT32(4)`: Length of the source string
- `PG_GETARG_BOOL(5)`: noError flag - when true, stops conversion on invalid characters instead of throwing errors

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL`: Extracts boolean argument from PostgreSQL function call
  - `PG_SJIS`: Encoding constant for Shift_JIS
  - `PG_MULE_INTERNAL`: Encoding constant for Mule Internal Code
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validates encoding conversion parameters
  - `[sjis2mic](sjis2mic.md)`: Core conversion function that performs the actual character-by-character conversion with complex mapping logic
  - `PG_RETURN_INT32`: Returns integer result to PostgreSQL
- Called from (representative examples):
  - `PGEUCALTCODE`: Referenced in the same source file

## Notes and Other Information
- This function is part of PostgreSQLs multi-byte character encoding conversion system
- The conversion adds leading codes (LC_JISX0201K, LC_JISX0208, LC_JISX0212) to create MIC-encoded output
- Handles sophisticated character range mappings: standard JIS X0208, UDC1 (user-defined characters mapping to X0208), UDC2 (mapping to X0212), and IBM kanji extensions
- Includes NEC selection IBM kanji handling with lookup table translation
- ASCII characters (0x00-0x7F) are passed through unchanged without leading codes
- The function follows PostgreSQLs V1 calling convention for user-defined functions
- Error handling can be controlled via the noError parameter to allow graceful handling of invalid character sequences
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c at lines 125-140