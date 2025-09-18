# mic2latin

## Location
src/backend/utils/mb/conv.c: 127 - 193

## Overview
Converts MIC (Multi-byte Internal Code) format back to Latin character encodings when the character codes map directly between the formats.

## Definition
```c
int mic2latin(const unsigned char *mic, 
              unsigned char *p, 
              int len,
              int lc, 
              int encoding, 
              bool noError)
```

## Detailed Description
The `mic2latin` function performs the reverse conversion of `latin2mic`, converting from PostgreSQL's internal MIC (Multi-byte Internal Code) format back to Latin character encodings. It processes MIC sequences by extracting ASCII characters directly and converting two-byte MIC sequences back to single-byte Latin characters. The function validates that MIC sequences are properly formed (exactly 2 bytes with correct language code) and that the second byte has the high bit set. This ensures data integrity during the conversion process.

## Parameters / Member Variables
- `mic`: Pointer to the source string in MIC format
- `p`: Output buffer for the converted Latin string
- `len`: Length of the source MIC string in bytes
- `lc`: Expected MIC language code identifier for the target Latin encoding
- `encoding`: PostgreSQL identifier for the target Latin encoding
- `noError`: Boolean flag controlling error handling - if true, conversion stops on error; if false, errors are reported

## Dependencies
- Functions called/Symbols referenced:
  - `[report_invalid_encoding](../r/report_invalid_encoding.md)`: Reports invalid character encoding errors
  - `PG_MULE_INTERNAL`: Constant identifier for MIC encoding
  - `IS_HIGHBIT_SET`: Macro to check if the high bit (0x80) is set in a character
  - `pg_mule_mblen`: Returns the byte length of a MIC character sequence
  - `[report_untranslatable_char](../r/report_untranslatable_char.md)`: Reports characters that cannot be translated between encodings

- Called from (representative examples):
  - `[mic_to_koi8r](mic_to_koi8r.md)`: MIC to KOI8-R conversion
  - `[mic_to_latin1](mic_to_latin1.md)`: MIC to Latin1 conversion
  - `[mic_to_latin2](mic_to_latin2.md)`: MIC to Latin2 conversion
  - `[mic_to_latin3](mic_to_latin3.md)`: MIC to Latin3 conversion
  - `[mic_to_latin4](mic_to_latin4.md)`: MIC to Latin4 conversion

## Notes and Other Information
- Returns the number of input bytes consumed, which may be less than input length if `noError` is true and an error occurs
- The output string is null-terminated
- ASCII characters (0x00-0x7F) are copied directly from MIC to Latin
- Two-byte MIC sequences are converted to single-byte Latin characters
- Validates MIC sequence integrity: must be exactly 2 bytes, first byte must match expected language code, second byte must have high bit set
- Part of PostgreSQL's bidirectional character encoding conversion system
- Provides the reverse operation to `latin2mic` for round-trip conversion capability
- The function ensures that only valid MIC sequences for the target Latin encoding are processed