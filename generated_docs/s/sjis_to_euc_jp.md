# sjis_to_euc_jp

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:77-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L77-L92)

## Overview
PostgreSQL encoding conversion function that converts text from Shift_JIS (SJIS) encoding to EUC-JP (Extended Unix Code for Japanese) encoding.

## Definition
```c
Datum sjis_to_euc_jp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL function wrapper for Shift_JIS to EUC-JP character encoding conversion. It extracts parameters from PostgreSQL function arguments, validates the encoding types, performs the actual conversion using the `sjis2euc_jp` helper function, and returns the number of bytes processed. The conversion handles complex Japanese character mappings including ASCII characters, JIS X0201 half-width katakana, JIS X0208 kanji, JIS X0212 kanji, user-defined characters (UDC1 and UDC2), and IBM-specific kanji characters. It uses lookup tables for IBM kanji conversions and properly handles multi-byte character boundaries.

## Parameters / Member Variables
The function uses PostgreSQLs `PG_FUNCTION_ARGS` macro to access arguments:
- `PG_GETARG_CSTRING(2)`: Source string in Shift_JIS encoding
- `PG_GETARG_CSTRING(3)`: Destination buffer for EUC-JP-encoded output
- `PG_GETARG_INT32(4)`: Length of the source string
- `PG_GETARG_BOOL(5)`: noError flag - when true, stops conversion on invalid characters instead of throwing errors

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL`: Extracts boolean argument from PostgreSQL function call
  - `PG_EUC_JP`: Encoding constant for EUC-JP
  - `PG_SJIS`: Encoding constant for Shift_JIS
  - `CHECK_ENCODING_CONVERSION_ARGS`: Validates encoding conversion parameters
  - [sjis2euc_jp](sjis2euc_jp.md): Core conversion function that performs the actual character-by-character conversion
  - `PG_RETURN_INT32`: Returns integer result to PostgreSQL
- Called from (representative examples):
  - `PGEUCALTCODE`: Referenced in the same source file

## Notes and Other Information
- This function is part of PostgreSQLs multi-byte character encoding conversion system
- The actual conversion logic handles complex mappings between Shift_JIS character ranges and EUC-JP equivalents
- Special handling for user-defined character areas (UDC1/UDC2) that map to extended JIS X0208 and JIS X0212 areas
- Includes sophisticated IBM kanji character mapping using lookup tables for NEC selection and other variants
- The function follows PostgreSQLs V1 calling convention for user-defined functions
- Error handling can be controlled via the noError parameter to allow graceful handling of invalid character sequences
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c at lines 77-92