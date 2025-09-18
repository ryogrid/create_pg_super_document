# mic_to_euc_jp

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 109 - 124

## Overview
PostgreSQL encoding conversion function that converts text from MIC (Mule Internal Code) encoding to EUC-JP (Extended Unix Code for Japanese) encoding.

## Definition
```c
Datum mic_to_euc_jp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function wrapper for MIC (Mule Internal Code) to EUC-JP character encoding conversion. It extracts parameters from PostgreSQL function arguments, validates the encoding types, performs the actual conversion using the `mic2euc_jp` helper function, and returns the number of bytes processed. The conversion process reverses the MIC encoding by interpreting leading codes (LC) and reconstructing the corresponding EUC-JP character sequences. ASCII characters pass through unchanged, LC_JISX0201K is converted to SS2 + katakana, LC_JISX0208 is converted to JIS X0208 kanji, and LC_JISX0212 is converted to SS3 + JIS X0212 kanji. The function includes proper error handling for untranslatable characters.

## Parameters / Member Variables
The function uses PostgreSQLs `PG_FUNCTION_ARGS` macro to access arguments:
- `PG_GETARG_CSTRING(2)`: Source string in MIC encoding
- `PG_GETARG_CSTRING(3)`: Destination buffer for EUC-JP-encoded output
- `PG_GETARG_INT32(4)`: Length of the source string
- `PG_GETARG_BOOL(5)`: noError flag - when true, stops conversion on invalid characters instead of throwing errors

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL`: Extracts boolean argument from PostgreSQL function call
  - `PG_EUC_JP`: Encoding constant for EUC-JP
  - `PG_MULE_INTERNAL`: Encoding constant for Mule Internal Code
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validates encoding conversion parameters
  - `[mic2euc_jp](mic2euc_jp.md)`: Core conversion function that performs the actual character-by-character conversion
  - `PG_RETURN_INT32`: Returns integer result to PostgreSQL
- Called from (representative examples):
  - `PGEUCALTCODE`: Referenced in the same source file

## Notes and Other Information
- This function is part of PostgreSQLs multi-byte character encoding conversion system
- Reverses the MIC encoding process by interpreting leading codes and reconstructing EUC-JP sequences
- Handles three main character types: JIS X0201 katakana (LC_JISX0201K → SS2), JIS X0208 kanji (LC_JISX0208), and JIS X0212 kanji (LC_JISX0212 → SS3)
- ASCII characters (0x00-0x7F) are passed through unchanged
- Includes error handling for untranslatable characters through `report_untranslatable_char`
- The function follows PostgreSQLs V1 calling convention for user-defined functions
- Error handling can be controlled via the noError parameter to allow graceful handling of invalid character sequences
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c at lines 109-124