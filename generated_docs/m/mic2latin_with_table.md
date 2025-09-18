# mic2latin_with_table

## Location
[src/backend/utils/mb/conv.c:257-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L257-L319)

## Overview
A generic single-byte charset encoding conversion function from MIC (Multi-byte Internal Code) to local charset using lookup tables for character translation.

## Definition
```c
int mic2latin_with_table(const unsigned char *mic,
                         unsigned char *p,
                         int len,
                         int lc,
                         int encoding,
                         const unsigned char *tab,
                         bool noError)
```

## Detailed Description
The `mic2latin_with_table` function converts from PostgreSQL's internal MIC format back to single-byte character encodings using a translation table. This function provides the reverse operation to `latin2mic_with_table`, handling complex character mappings that don't follow direct correspondence. It processes ASCII characters directly while using the lookup table to translate the second byte of two-byte MIC sequences back to local charset characters. The function validates MIC sequence integrity and ensures that table entries exist for successful translation.

## Parameters / Member Variables
- `mic`: Pointer to the source string in MIC format
- `p`: Output buffer for the converted local charset string
- `len`: Length of the source MIC string in bytes
- `lc`: Expected MIC language code identifier for the target local encoding
- `encoding`: PostgreSQL identifier for the target local encoding
- `tab`: Translation table for MIC's second byte starting from character 128 (0x80), where each entry contains the corresponding local charset code point or 0 if no equivalent exists
- `noError`: Boolean flag controlling error handling - if true, conversion stops on error; if false, errors are reported

## Dependencies
- Functions called/Symbols referenced:
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Reports invalid character encoding errors
  - `PG_MULE_INTERNAL`: Constant identifier for MIC encoding
  - `IS_HIGHBIT_SET`: Macro to check if the high bit (0x80) is set in a character
  - `pg_mule_mblen`: Returns the byte length of a MIC character sequence
  - `HIGHBIT`: Constant representing the high bit value (0x80)
  - [report_untranslatable_char](../r/report_untranslatable_char.md): Reports characters that cannot be translated between encodings

- Called from (representative examples):
  - [mic_to_iso](mic_to_iso.md): MIC to ISO encoding conversion
  - [mic_to_win1251](mic_to_win1251.md): MIC to Windows-1251 conversion
  - [mic_to_win866](mic_to_win866.md): MIC to Windows-866 conversion
  - [mic_to_win1250](mic_to_win1250.md): MIC to Windows-1250 conversion

## Notes and Other Information
- Returns the number of input bytes consumed, which may be less than input length if `noError` is true and an error occurs
- The output string is null-terminated
- ASCII characters are copied directly from MIC to the target encoding
- Two-byte MIC sequences are validated and their second byte is translated using the lookup table
- Validates MIC sequence integrity: must be exactly 2 bytes, first byte must match expected language code, second byte must have high bit set, and table entry must exist (non-zero)
- Characters with no table mapping (table entry = 0) trigger translation errors
- Provides the reverse operation to `latin2mic_with_table` for round-trip conversion capability
- Essential for complex MIC to single-byte encoding conversions where direct mapping is not possible
- Part of PostgreSQL's bidirectional character encoding conversion system