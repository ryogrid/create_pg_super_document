# euc_jp2sjis

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:534-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L534-L637)

## Overview
Converts text from EUC-JP (Extended Unix Code for Japanese) encoding to Shift JIS (SJIS) encoding, handling various Japanese character sets including ASCII, JIS X0201 kana, JIS X0208 kanji, JIS X0212 kanji, and user-defined characters.

## Definition
static int euc_jp2sjis(const unsigned char *euc, unsigned char *p, int len, bool noError)

## Detailed Description
This function performs character-by-character conversion from EUC-JP to Shift JIS encoding. It processes different types of Japanese characters:

- **ASCII characters**: Copied directly without conversion
- **JIS X0201 half-width katakana**: Identified by SS2 prefix, converted by removing the SS2 prefix
- **JIS X0208 kanji**: Main Japanese character set, converted using standard EUC to SJIS mathematical transformation
- **JIS X0212 kanji**: Extended character set identified by SS3 prefix, handled through lookup tables or UDC2 mapping
- **User-defined characters (UDC1/UDC2)**: Custom character ranges mapped to specific SJIS code points
- **IBM extended kanji**: Special IBM character extensions handled via lookup table

The function includes comprehensive error handling and can operate in two modes: strict (reports encoding errors) or lenient (stops at first invalid sequence).

## Parameters / Member Variables
- : Source buffer containing EUC-JP encoded text to convert
- : Destination buffer where the converted SJIS text will be written
- : Number of bytes remaining in the source buffer to process
- : If true, stops conversion at first invalid sequence; if false, reports encoding errors via report_invalid_encoding

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)
  - PG_EUC_JP
  - SS2 (Single Shift 2 - JIS X0201 kana prefix)
  - SS3 (Single Shift 3 - JIS X0212 kanji prefix)  
  - PGSJISALTCODE (alternative character code for unmappable characters)
  - ibmkanji (lookup table for IBM extended characters)
- Called from (representative examples):
  - [euc_jp_to_sjis](euc_jp_to_sjis.md) (public conversion function)

## Notes and Other Information
- The function null-terminates the output buffer
- Returns the number of bytes processed from the input buffer
- Handles multi-byte character boundary validation using pg_encoding_verifymbchar
- Uses mathematical formulas for standard JIS X0208 character conversion: ((c1 - 0xa1) >> 1) + ((c1 < 0xdf) ? 0x81 : 0xc1)
- Special handling for User Defined Character areas (UDC1: 0xf5a1+ in JIS X0208, UDC2: 0xf5a1+ in JIS X0212)
- IBM kanji characters are mapped through a lookup table (ibmkanji array) with fallback to PGSJISALTCODE for unmappable characters
- The conversion process preserves character boundaries and validates input encoding integrity

## Simplified Source

```c
static int euc_jp2sjis(const unsigned char *euc, unsigned char *p, int len, bool noError) {
    const unsigned char *start = euc;
    int c1, c2, k, l;

    while (len > 0) {
        c1 = *euc;

        // Handle ASCII characters
        if (!IS_HIGHBIT_SET(c1)) {
            if (c1 == 0) {
                if (!noError) {
                    report_invalid_encoding(PG_EUC_JP, (const char *) euc, len);
                }
                break;
            }
            *p++ = c1;
            euc++;
            len--;
            continue;
        }

        // Verify EUC-JP character sequence length
        l = pg_encoding_verifymbchar(PG_EUC_JP, (const char *) euc, len);
        if (l < 0) {
            if (!noError) {
                report_invalid_encoding(PG_EUC_JP, (const char *) euc, len);
            }
            break;
        }

        // Handle JIS X0201 half-width katakana (SS2 prefix)
        if (c1 == SS2) {
            *p++ = euc[1];  // Remove SS2 prefix, copy katakana byte
        }
        // Handle JIS X0212 supplementary kanji (SS3 prefix)
        else if (c1 == SS3) {
            c1 = euc[1];
            c2 = euc[2];
            k = c1 << 8 | c2;

            // Check for user-defined characters 2 (UDC2)
            if (k >= 0xf5a1) {
                c1 -= 0x54;
                *p++ = ((c1 - 0xa1) >> 1) + ((c1 < 0xdf) ? 0x81 : 0xc1) + 0x74;
                *p++ = c2 - ((c1 & 1) ? ((c2 < 0xe0) ? 0x61 : 0x60) : 2);
            } else {
                // IBM kanji lookup
                int i, k2;
                for (i = 0; ; i++) {
                    k2 = ibmkanji[i].euc & 0xffff;
                    if (k2 == 0xffff) {
                        // Use alternative code if no mapping found
                        *p++ = PGSJISALTCODE >> 8;
                        *p++ = PGSJISALTCODE & 0xff;
                        break;
                    }
                    if (k2 == k) {
                        k = ibmkanji[i].sjis;
                        *p++ = k >> 8;
                        *p++ = k & 0xff;
                        break;
                    }
                }
            }
        }
        // Handle JIS X0208 kanji/kana (2 bytes with high bits)
        else {
            c2 = euc[1];
            k = (c1 << 8) | (c2 & 0xff);

            // Check for user-defined characters 1 (UDC1)
            if (k >= 0xf5a1) {
                c1 -= 0x54;
                *p++ = ((c1 - 0xa1) >> 1) + ((c1 < 0xdf) ? 0x81 : 0xc1) + 0x6f;
            } else {
                // Standard JIS X0208 conversion
                *p++ = ((c1 - 0xa1) >> 1) + ((c1 < 0xdf) ? 0x81 : 0xc1);
            }
            *p++ = c2 - ((c1 & 1) ? ((c2 < 0xe0) ? 0x61 : 0x60) : 2);
        }

        euc += l;
        len -= l;
    }

    *p = '\0';
    return euc - start;
}
```