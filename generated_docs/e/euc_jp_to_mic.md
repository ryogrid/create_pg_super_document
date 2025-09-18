# euc_jp_to_mic

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 93 - 108

## Overview
PostgreSQL encoding conversion function that converts text from EUC-JP (Extended Unix Code for Japanese) encoding to MIC (Mule Internal Code) encoding.

## Definition
```c
Datum euc_jp_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function wrapper for EUC-JP to MIC (Mule Internal Code) character encoding conversion. It extracts parameters from PostgreSQL function arguments, validates the encoding types, performs the actual conversion using the `euc_jp2mic` helper function, and returns the number of bytes processed. The MIC encoding is PostgreSQLs internal multi-byte encoding format used as an intermediate representation for character set conversions. The conversion process adds leading code (LC) bytes to identify different character sets: ASCII characters pass through unchanged, JIS X0201 katakana gets LC_JISX0201K prefix, JIS X0208 kanji gets LC_JISX0208 prefix, and JIS X0212 kanji gets LC_JISX0212 prefix.

## Parameters / Member Variables
The function uses PostgreSQLs `PG_FUNCTION_ARGS` macro to access arguments:
- `PG_GETARG_CSTRING(2)`: Source string in EUC-JP encoding
- `PG_GETARG_CSTRING(3)`: Destination buffer for MIC-encoded output
- `PG_GETARG_INT32(4)`: Length of the source string
- `PG_GETARG_BOOL(5)`: noError flag - when true, stops conversion on invalid characters instead of throwing errors

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL`: Extracts boolean argument from PostgreSQL function call
  - `PG_EUC_JP`: Encoding constant for EUC-JP
  - `PG_MULE_INTERNAL`: Encoding constant for Mule Internal Code
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validates encoding conversion parameters
  - `[euc_jp2mic](euc_jp2mic.md)`: Core conversion function that performs the actual character-by-character conversion
  - `PG_RETURN_INT32`: Returns integer result to PostgreSQL
- Called from (representative examples):
  - `PGEUCALTCODE`: Referenced in the same source file

## Notes and Other Information
- This function is part of PostgreSQLs multi-byte character encoding conversion system
- MIC (Mule Internal Code) serves as an intermediate encoding format in PostgreSQLs conversion framework
- The conversion adds leading codes (LC_JISX0201K, LC_JISX0208, LC_JISX0212) to distinguish different Japanese character sets
- ASCII characters (0x00-0x7F) are passed through unchanged without leading codes
- The function follows PostgreSQLs V1 calling convention for user-defined functions
- Error handling can be controlled via the noError parameter to allow graceful handling of invalid character sequences
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c at lines 93-108