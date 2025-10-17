# latin2mic_with_table

## Location
[src/backend/utils/mb/conv.c:194-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L194-L256)

## Overview
A generic single-byte charset encoding conversion function from local charset to MIC (Multi-byte Internal Code) using lookup tables for character translation.

## Definition
```c
int latin2mic_with_table(const unsigned char *l,
                         unsigned char *p,
                         int len,
                         int lc,
                         int encoding,
                         const unsigned char *tab,
                         bool noError)
```

## Detailed Description
The `latin2mic_with_table` function converts single-byte character encodings to PostgreSQL's internal MIC format using a translation table for high-bit characters. Unlike the simpler `latin2mic` function that assumes direct mapping, this function uses a lookup table to handle encodings where character codes don't map directly to MIC. ASCII characters (0x00-0x7F) are copied directly, while high-bit characters (0x80-0xFF) are looked up in the provided table and converted to two-byte MIC sequences (language code + translated character). This approach supports complex character set conversions that require character code remapping.

## Parameters / Member Variables
- `l`: Pointer to the source string in the local charset
- `p`: Output buffer for the converted MIC string (must be large enough to accommodate expansion)
- `len`: Length of the source string in bytes
- `lc`: MIC language code identifier for the source encoding
- `encoding`: PostgreSQL identifier for the source encoding
- `tab`: Translation table starting from character 128 (0x80), where each entry contains the corresponding MIC code point or 0 if no equivalent exists
- `noError`: Boolean flag controlling error handling - if true, conversion stops on error; if false, errors are reported

## Dependencies
- Functions called/Symbols referenced:
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Reports invalid character encoding errors
  - `IS_HIGHBIT_SET`: Macro to check if the high bit (0x80) is set in a character
  - `HIGHBIT`: Constant representing the high bit value (0x80)
  - [report_untranslatable_char](../r/report_untranslatable_char.md): Reports characters that cannot be translated between encodings
  - `PG_MULE_INTERNAL`: Constant identifier for MIC encoding

- Called from (representative examples):
  - [iso_to_mic](../i/iso_to_mic.md): ISO encoding to MIC conversion
  - [win1251_to_mic](../w/win1251_to_mic.md): Windows-1251 to MIC conversion
  - [win866_to_mic](../w/win866_to_mic.md): Windows-866 to MIC conversion
  - [win1250_to_mic](../w/win1250_to_mic.md): Windows-1250 to MIC conversion

## Notes and Other Information
- Returns the number of input bytes consumed, which may be less than input length if `noError` is true and an error occurs
- The output string is null-terminated
- High-bit characters result in two-byte MIC sequences when a valid table entry exists
- Characters with no table mapping (table entry = 0) trigger translation errors
- The output buffer must be sized to accommodate potential expansion (up to 2x input size for strings with many high-bit characters)
- This function is more flexible than `latin2mic` as it supports encodings that require character code remapping
- Used for complex single-byte to MIC conversions where direct mapping is not possible
- Part of PostgreSQL's comprehensive character encoding conversion infrastructure

## Simplified Source

```c
int latin2mic_with_table(const unsigned char *l, unsigned char *p, int len,
                         int lc, int encoding, const unsigned char *tab,
                         bool noError)
{
    const unsigned char *start = l;

    while (len > 0) {
        unsigned char c1 = *l;

        // Check for null byte (invalid)
        if (c1 == 0) {
            if (noError) break;
            report_invalid_encoding(encoding, (const char *) l, len);
        }

        // ASCII characters: copy directly
        if (!IS_HIGHBIT_SET(c1)) {
            *p++ = c1;
        }
        // High-bit characters: use translation table
        else {
            unsigned char c2 = tab[c1 - HIGHBIT];
            if (c2) {
                // Create 2-byte MIC sequence
                *p++ = lc;  // MIC language code
                *p++ = c2;  // Translated character
            } else {
                if (noError) break;
                report_untranslatable_char(encoding, PG_MULE_INTERNAL,
                                         (const char *) l, len);
            }
        }

        l++;
        len--;
    }

    *p = '\0';
    return l - start;  // Return bytes consumed
}
```