# latin2mic

## Location
[src/backend/utils/mb/conv.c:89-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L89-L126)

## Overview
Converts Latin character encodings to MIC (Multi-byte Internal Code) format when the local character codes map directly to MIC codes.

## Definition
```c
int latin2mic(const unsigned char *l, 
              unsigned char *p, 
              int len,
              int lc, 
              int encoding, 
              bool noError)
```

## Detailed Description
The `latin2mic` function performs conversion from Latin character encodings (such as Latin1, Latin2, Latin3, Latin4) to PostgreSQL's internal MIC (Multi-byte Internal Code) format. This conversion is used when the source encoding's character codes can be directly mapped to MIC without requiring complex translation tables. For high-bit characters (0x80-0xFF), the function prepends the MIC language code before the original character byte, effectively creating a two-byte MIC sequence. ASCII characters (0x00-0x7F) are copied directly without modification.

## Parameters / Member Variables
- `l`: Pointer to the source string in Latin encoding
- `p`: Output buffer for the converted MIC string (must be large enough to accommodate expansion)
- `len`: Length of the source string in bytes
- `lc`: MIC language code identifier for the specific Latin encoding being converted
- `encoding`: PostgreSQL identifier for the source Latin encoding
- `noError`: Boolean flag controlling error handling - if true, conversion stops on error; if false, errors are reported

## Dependencies
- Functions called/Symbols referenced:
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Reports invalid character encoding errors
  - `IS_HIGHBIT_SET`: Macro to check if the high bit (0x80) is set in a character

- Called from (representative examples):
  - [koi8r_to_mic](../k/koi8r_to_mic.md): KOI8-R to MIC conversion
  - [latin1_to_mic](latin1_to_mic.md): Latin1 to MIC conversion
  - [latin2_to_mic](latin2_to_mic.md): Latin2 to MIC conversion
  - [latin3_to_mic](latin3_to_mic.md): Latin3 to MIC conversion
  - [latin4_to_mic](latin4_to_mic.md): Latin4 to MIC conversion

## Notes and Other Information
- Returns the number of input bytes consumed, which may be less than input length if `noError` is true and an error occurs
- The output string is null-terminated
- High-bit characters result in two-byte MIC sequences (language code + original byte)
- ASCII characters are preserved as single bytes in the output
- This function handles direct mapping scenarios where no complex character translation is needed
- Part of PostgreSQL's comprehensive character encoding conversion system
- The output buffer must be sized to accommodate potential expansion (up to 2x input size for strings with many high-bit characters)

## Simplified Source

```c
int latin2mic(const unsigned char *l, unsigned char *p, int len,
              int lc, int encoding, bool noError)
{
    const unsigned char *start = l;

    while (len > 0) {
        int c1 = *l;

        // Check for null byte (invalid)
        if (c1 == 0) {
            if (noError) break;
            report_invalid_encoding(encoding, (const char *) l, len);
        }

        // High-bit characters: prepend MIC language code
        if (IS_HIGHBIT_SET(c1)) {
            *p++ = lc;  // MIC language code first
        }

        // Copy the original character
        *p++ = c1;

        l++;
        len--;
    }

    *p = '\0';
    return l - start;  // Return bytes consumed
}
```