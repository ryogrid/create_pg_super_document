# euc_jis_20042shift_jis_2004

## Location
[src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:75-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c#L75-L221)

## Overview
Core conversion function that performs the actual character-by-character conversion from EUC-JIS-2004 encoding to Shift-JIS-2004 encoding.

## Definition
```c
static int euc_jis_20042shift_jis_2004(const unsigned char *euc, unsigned char *p, int len, bool noError)
```

## Detailed Description
This is the core implementation function that handles the complex logic of converting EUC-JIS-2004 encoded text to Shift-JIS-2004 encoding. It processes the input byte by byte, handling different character planes and ranges according to JIS X 0213 specifications. The function handles ASCII characters, JIS X 0201 kana characters (plane 1), JIS X 0213 plane 1, and JIS X 0213 plane 2 characters. It performs mathematical transformations on ku (row) and ten (column) values to map between the two encoding schemes.

## Parameters / Member Variables
- `euc`: Pointer to the source string in EUC-JIS-2004 encoding
- `p`: Pointer to the destination buffer for converted Shift-JIS-2004 string
- `len`: Length of the source string in bytes
- `noError`: If true, stops conversion on error instead of throwing exception

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)
  - Constants: PG_EUC_JIS_2004, SS2, SS3
- Called from:
  - [euc_jis_2004_to_shift_jis_2004](euc_jis_2004_to_shift_jis_2004.md) (wrapper function)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:75-221
- Static function - only accessible within the same compilation unit
- Returns the number of bytes processed from the source string
- Handles multiple character encoding planes:
  - ASCII characters (0x00-0x7F) - passed through unchanged
  - SS2 sequences (JIS X 0201 kana) - single byte output
  - SS3 sequences (JIS X 0213 plane 2) - two byte output with complex mapping
  - Regular sequences (JIS X 0213 plane 1) - two byte output
- Uses mathematical formulas to convert ku (row) and ten (column) positions between encodings
- Implements comprehensive error checking and validation
- Null-terminates the output string

## Simplified Source

```c
static int euc_jis_20042shift_jis_2004(const unsigned char *euc, unsigned char *p, int len, bool noError) {
    const unsigned char *start = euc;
    int c1, ku, ten, l;

    while (len > 0) {
        c1 = *euc;

        // Handle ASCII characters (0x00-0x7F)
        if (!IS_HIGHBIT_SET(c1)) {
            if (c1 == 0 && !noError) {
                report_invalid_encoding(PG_EUC_JIS_2004, (const char *) euc, len);
            }
            *p++ = c1;  // Copy ASCII unchanged
            euc++; len--;
            continue;
        }

        // Verify multi-byte character validity
        l = pg_encoding_verifymbchar(PG_EUC_JIS_2004, (const char *) euc, len);
        if (l < 0) {
            if (noError) break;
            report_invalid_encoding(PG_EUC_JIS_2004, (const char *) euc, len);
        }

        // Handle JIS X 0201 kana (SS2 sequences)
        if (c1 == SS2 && l == 2) {
            *p++ = euc[1];  // Single byte output
        }

        // Handle JIS X 0213 plane 2 (SS3 sequences)
        else if (c1 == SS3 && l == 3) {
            ku = euc[1] - 0xa0;
            ten = euc[2] - 0xa0;

            // Map ku (row) to first byte with special cases
            switch (ku) {
                case 1: case 3: case 4: case 5: case 8: case 12: case 13: case 14: case 15:
                    *p++ = ((ku + 0x1df) >> 1) - (ku >> 3) * 3;
                    break;
                default:
                    if (ku >= 78 && ku <= 94) {
                        *p++ = (ku + 0x19b) >> 1;
                    } else {
                        if (!noError) report_invalid_encoding(PG_EUC_JIS_2004, (const char *) euc, len);
                        break;
                    }
            }

            // Map ten (column) to second byte
            if (ku % 2) {
                if (ten >= 1 && ten <= 63) *p++ = ten + 0x3f;
                else if (ten >= 64 && ten <= 94) *p++ = ten + 0x40;
                else if (!noError) report_invalid_encoding(PG_EUC_JIS_2004, (const char *) euc, len);
            } else {
                *p++ = ten + 0x9e;
            }
        }

        // Handle JIS X 0213 plane 1 (regular 2-byte sequences)
        else if (l == 2) {
            ku = c1 - 0xa0;
            ten = euc[1] - 0xa0;

            // Map ku to first byte
            if (ku >= 1 && ku <= 62) *p++ = (ku + 0x101) >> 1;
            else if (ku >= 63 && ku <= 94) *p++ = (ku + 0x181) >> 1;
            else {
                if (!noError) report_invalid_encoding(PG_EUC_JIS_2004, (const char *) euc, len);
                break;
            }

            // Map ten to second byte (same logic as plane 2)
            if (ku % 2) {
                if (ten >= 1 && ten <= 63) *p++ = ten + 0x3f;
                else if (ten >= 64 && ten <= 94) *p++ = ten + 0x40;
                else if (!noError) report_invalid_encoding(PG_EUC_JIS_2004, (const char *) euc, len);
            } else {
                *p++ = ten + 0x9e;
            }
        }

        // Invalid sequence
        else {
            if (!noError) report_invalid_encoding(PG_EUC_JIS_2004, (const char *) euc, len);
            break;
        }

        euc += l;
        len -= l;
    }

    *p = '\0';  // Null terminate output
    return euc - start;  // Return bytes processed
}
```