# shift_jis_20042euc_jis_2004

## Location
[src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:254-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c#L254-L401)

## Overview
Core conversion function that performs the actual character-by-character conversion from Shift-JIS-2004 encoding to EUC-JIS-2004 encoding.

## Definition
```c
static int shift_jis_20042euc_jis_2004(const unsigned char *sjis, unsigned char *p, int len, bool noError)
```

## Detailed Description
This is the core implementation function that handles the complex reverse conversion from Shift-JIS-2004 encoded text to EUC-JIS-2004 encoding. It processes the input byte by byte, identifying different character types and planes, then applies the appropriate mathematical transformations to convert ku (row) and ten (column) values back to EUC encoding format. The function handles ASCII characters, JIS X 0201 kana, JIS X 0213 plane 1, and JIS X 0213 plane 2 characters, using the `get_ten` helper function to decode Shift-JIS-2004 byte values.

## Parameters / Member Variables
- `sjis`: Pointer to the source string in Shift-JIS-2004 encoding
- `p`: Pointer to the destination buffer for converted EUC-JIS-2004 string
- `len`: Length of the source string in bytes
- `noError`: If true, stops conversion on error instead of throwing exception

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)
  - [get_ten](../g/get_ten.md) (called 4 times at lines 314, 327, 341, 368)
  - Constants: PG_SHIFT_JIS_2004, SS2, SS3
- Called from:
  - [shift_jis_2004_to_euc_jis_2004](shift_jis_2004_to_euc_jis_2004.md) (wrapper function)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:254-401
- Static function - only accessible within the same compilation unit
- Returns the number of bytes processed from the source string
- Handles multiple character encoding categories:
  - ASCII characters (0x00-0x7F) - passed through unchanged
  - JIS X 0201 kana (0xA1-0xDF) - converted to SS2 + character
  - Plane 1 characters (0x81-0x9F, 0xE0-0xEF) - mapped to regular EUC sequences
  - Plane 2 characters (0xF0-0xFC) - mapped to SS3 + EUC sequences with complex ku mapping
- Uses complex mathematical formulas and lookup tables for byte range conversions
- Implements comprehensive error checking and validation
- Null-terminates the output string
- Counterpart to `euc_jis_20042shift_jis_2004` function

## Simplified Source

```c
static int shift_jis_20042euc_jis_2004(const unsigned char *sjis, unsigned char *p, int len, bool noError) {
    const unsigned char *start = sjis;
    int c1, ku, ten, kubun, plane, l;

    while (len > 0) {
        c1 = *sjis;

        // Handle ASCII characters (0x00-0x7F)
        if (!IS_HIGHBIT_SET(c1)) {
            if (c1 == 0 && !noError) {
                report_invalid_encoding(PG_SHIFT_JIS_2004, (const char *) sjis, len);
            }
            *p++ = c1;
            sjis++;
            len--;
            continue;
        }

        // Verify multibyte character validity
        l = pg_encoding_verifymbchar(PG_SHIFT_JIS_2004, (const char *) sjis, len);
        if (l < 0 || l > len) {
            if (!noError) report_invalid_encoding(PG_SHIFT_JIS_2004, (const char *) sjis, len);
            break;
        }

        // Handle JIS X0201 kana (1-byte, 0xA1-0xDF)
        if (c1 >= 0xa1 && c1 <= 0xdf && l == 1) {
            *p++ = SS2;  // Single shift 2
            *p++ = c1;
        }
        // Handle 2-byte characters (JIS X 0213)
        else if (l == 2) {
            int c2 = sjis[1];
            plane = 1;

            // Determine ku (row) and ten (column) based on byte ranges
            if (c1 >= 0x81 && c1 <= 0x9f) {          // Plane 1: 1ku-62ku
                ku = (c1 << 1) - 0x100;
                ten = get_ten(c2, &kubun);
                if (ten >= 0) ku -= kubun;
            }
            else if (c1 >= 0xe0 && c1 <= 0xef) {     // Plane 1: 62ku-94ku
                ku = (c1 << 1) - 0x180;
                ten = get_ten(c2, &kubun);
                if (ten >= 0) ku -= kubun;
            }
            else if (c1 >= 0xf0 && c1 <= 0xf3) {     // Plane 2: special ku mapping
                plane = 2;
                ten = get_ten(c2, &kubun);
                if (ten >= 0) {
                    // Map special ku values based on c1 and kubun
                    if (c1 == 0xf0) ku = kubun == 0 ? 8 : 1;
                    else if (c1 == 0xf1) ku = kubun == 0 ? 4 : 3;
                    else if (c1 == 0xf2) ku = kubun == 0 ? 12 : 5;
                    else ku = kubun == 0 ? 14 : 13;
                }
            }
            else if (c1 >= 0xf4 && c1 <= 0xfc) {     // Plane 2: 78-94ku
                plane = 2;
                ten = get_ten(c2, &kubun);
                if (ten >= 0) {
                    ku = (c1 == 0xf4 && kubun == 1) ? 15 : (c1 << 1) - 0x19a - kubun;
                }
            }

            // Check for conversion errors
            if (ten < 0) {
                if (!noError) report_invalid_encoding(PG_SHIFT_JIS_2004, (const char *) sjis, len);
                break;
            }

            // Output EUC encoding
            if (plane == 2) *p++ = SS3;  // Single shift 3 for plane 2
            *p++ = ku + 0xa0;            // Add EUC offset to ku
            *p++ = ten + 0xa0;           // Add EUC offset to ten
        }

        sjis += l;
        len -= l;
    }

    *p = '\0';
    return sjis - start;
}
```