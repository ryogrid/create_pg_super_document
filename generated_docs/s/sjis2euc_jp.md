# sjis2euc_jp

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:638-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L638-L772)

## Overview
Converts text from Shift JIS (SJIS) encoding to EUC-JP (Extended Unix Code for Japanese) encoding, handling various Japanese character sets including ASCII, JIS X0201 kana, JIS X0208 kanji, and IBM extended characters.

## Definition
static int sjis2euc_jp(const unsigned char *sjis, unsigned char *p, int len, bool noError)

## Detailed Description
This function performs character-by-character conversion from Shift JIS to EUC-JP encoding. It processes different categories of Japanese characters:

- **ASCII characters**: Copied directly without conversion
- **JIS X0201 half-width katakana**: Characters in range 0xa1-0xdf, converted by adding SS2 prefix
- **JIS X0208 kanji**: Main Japanese character set, converted using standard SJIS to EUC mathematical transformation
- **NEC selection IBM kanji**: Special IBM character extensions (0xed40-0xf040 range) handled via lookup table
- **User-defined characters**: Two ranges (UDC1: 0xf040-0xf540, UDC2: 0xf540-0xfa40) mapped to EUC-JP extended areas
- **IBM kanji**: Characters ≥0xfa40 mapped through lookup table to either JIS X0208 or JIS X0212

The function includes comprehensive error handling and supports both strict and lenient conversion modes.

## Parameters / Member Variables
- : Source buffer containing SJIS encoded text to convert
- : Destination buffer where the converted EUC-JP text will be written  
- : Number of bytes remaining in the source buffer to process
- : If true, stops conversion at first invalid sequence; if false, reports encoding errors via report_invalid_encoding

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)
  - PG_SJIS
  - SS2 (Single Shift 2 - prefix for JIS X0201 kana in EUC-JP)
  - SS3 (Single Shift 3 - prefix for JIS X0212 kanji in EUC-JP)
  - PGEUCALTCODE (alternative character code for unmappable characters)
  - ibmkanji (lookup table for IBM extended characters with .nec, .sjis, and .euc fields)
- Called from (representative examples):
  - [sjis_to_euc_jp](sjis_to_euc_jp.md) (public conversion function)

## Notes and Other Information
- The function null-terminates the output buffer
- Returns the number of bytes processed from the input buffer
- Handles multi-byte character boundary validation using pg_encoding_verifymbchar
- Uses mathematical formulas for standard JIS X0208 character conversion: ((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e)
- **UDC1 mapping**: SJIS 0xf040-0xf540 maps to EUC-JP X0208 85-94 ku (0xf5a1-0xfefe)
- **UDC2 mapping**: SJIS 0xf540-0xfa40 maps to EUC-JP X0212 85-94 ku (SS3 + 0xf5a1-0xfefe)
- **NEC selection**: Special handling for NEC-specific IBM kanji characters in 0xeb40-0xf040 range
- **IBM kanji lookup**: Uses ibmkanji array to map IBM extended characters, with special handling for X0212 characters (indicated by high byte 0x8f)
- Unmappable characters are replaced with PGEUCALTCODE placeholder
- The conversion preserves character boundaries and validates input encoding integrity

## Simplified Source

```c
static int sjis2euc_jp(const unsigned char *sjis, unsigned char *p, int len, bool noError) {
    const unsigned char *start = sjis;
    int c1, c2, i, k, k2, l;

    while (len > 0) {
        c1 = *sjis;

        // Handle ASCII characters
        if (!IS_HIGHBIT_SET(c1)) {
            if (c1 == 0) {
                if (!noError) {
                    report_invalid_encoding(PG_SJIS, (const char *) sjis, len);
                }
                break;
            }
            *p++ = c1;
            sjis++;
            len--;
            continue;
        }

        // Verify SJIS character sequence length
        l = pg_encoding_verifymbchar(PG_SJIS, (const char *) sjis, len);
        if (l < 0) {
            if (!noError) {
                report_invalid_encoding(PG_SJIS, (const char *) sjis, len);
            }
            break;
        }

        // Handle JIS X0201 katakana (single-byte 0xA1-0xDF)
        if (c1 >= 0xa1 && c1 <= 0xdf) {
            *p++ = SS2;     // Add SS2 prefix for EUC-JP
            *p++ = c1;
        }
        // Handle multi-byte Japanese characters
        else {
            c2 = sjis[1];
            k = (c1 << 8) + c2;

            // Handle NEC selection IBM kanji conversion
            if (k >= 0xed40 && k < 0xf040) {
                for (i = 0; ; i++) {
                    k2 = ibmkanji[i].nec;
                    if (k2 == 0xffff) break;
                    if (k2 == k) {
                        k = ibmkanji[i].sjis;
                        c1 = (k >> 8) & 0xff;
                        c2 = k & 0xff;
                        break;
                    }
                }
            }

            // Convert based on character range
            if (k < 0xeb3f) {
                // Standard JIS X0208 characters
                *p++ = ((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e);
                *p++ = c2 + ((c2 > 0x9e) ? 2 : 0x60) + (c2 < 0x80);
            }
            else if ((k >= 0xeb40 && k < 0xf040) || (k >= 0xfc4c && k <= 0xfcfc)) {
                // NEC/IBM extension characters - use alternative code
                *p++ = PGEUCALTCODE >> 8;
                *p++ = PGEUCALTCODE & 0xff;
            }
            else if (k >= 0xf040 && k < 0xf540) {
                // User-defined characters 1 (UDC1) -> JIS X0208
                c1 -= 0x6f;
                *p++ = ((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e);
                *p++ = c2 + ((c2 > 0x9e) ? 2 : 0x60) + (c2 < 0x80);
            }
            else if (k >= 0xf540 && k < 0xfa40) {
                // User-defined characters 2 (UDC2) -> JIS X0212
                *p++ = SS3;     // Add SS3 prefix for JIS X0212
                c1 -= 0x74;
                *p++ = ((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e);
                *p++ = c2 + ((c2 > 0x9e) ? 2 : 0x60) + (c2 < 0x80);
            }
            else if (k >= 0xfa40) {
                // IBM kanji mapping
                for (i = 0; ; i++) {
                    k2 = ibmkanji[i].sjis;
                    if (k2 == 0xffff) break;
                    if (k2 == k) {
                        k = ibmkanji[i].euc;
                        if (k >= 0x8f0000) {
                            // JIS X0212 character
                            *p++ = SS3;
                            *p++ = 0x80 | ((k & 0xff00) >> 8);
                            *p++ = 0x80 | (k & 0xff);
                        } else {
                            // JIS X0208 character
                            *p++ = 0x80 | (k >> 8);
                            *p++ = 0x80 | (k & 0xff);
                        }
                        break;
                    }
                }
            }
        }

        sjis += l;
        len -= l;
    }

    *p = '\0';
    return sjis - start;
}
```