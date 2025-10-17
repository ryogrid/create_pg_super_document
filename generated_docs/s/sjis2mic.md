# sjis2mic

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:160-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L160-L298)

## Overview
Core conversion function that transforms Japanese Shift JIS (SJIS) encoded text to PostgreSQL's Mule Internal Code (MIC) encoding, handling various Japanese character sets including JIS X0208, X0212, and user-defined characters.

## Definition

```c
static int
sjis2mic(const unsigned char *sjis, unsigned char *p, int len, bool noError)
```
## Detailed Description
This function performs the complex conversion from Shift JIS encoding to Mule Internal Code. It processes different types of Japanese characters including single-byte half-width katakana (JIS X0201), double-byte kanji and hiragana/katakana (JIS X0208), supplementary kanji (JIS X0212), and various user-defined character areas. The function also handles special IBM kanji mappings and NEC selection characters. It uses lookup tables and algorithmic conversion to map SJIS byte sequences to appropriate MIC character codes with proper language character (LC) prefixes.

## Parameters / Member Variables
- `*sjis`: Source string in Shift JIS encoding to be converted
- `*p`: Destination buffer where MIC encoded output will be written
- `len`: Length of the source SJIS string in bytes
- `noError`: Boolean flag indicating whether to suppress error reporting for invalid sequences
## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET: Check if character has high bit set
  - ISSJISHEAD: Validate SJIS lead byte
  - ISSJISTAIL: Validate SJIS trail byte
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report encoding conversion errors
  - LC_JISX0201K: Language character code for JIS X0201 katakana
  - LC_JISX0208: Language character code for JIS X0208
  - LC_JISX0212: Language character code for JIS X0212
  - PGEUCALTCODE: Alternative encoding code for unmappable characters
  - ibmkanji: Lookup table for IBM kanji mappings
- Called from (representative examples):
  - [sjis_to_mic](sjis_to_mic.md): PostgreSQL function wrapper for SJIS to MIC conversion
  - PGEUCALTCODE: Referenced in encoding conversion system

## Notes and Other Information
- Handles multiple Japanese character encoding standards within SJIS
- Supports conversion of JIS X0201 single-byte katakana (0xa1-0xdf range)
- Processes JIS X0208 kanji and kana using algorithmic conversion
- Maps user-defined characters (UDC1, UDC2) to appropriate JIS character sets
- Includes special handling for IBM kanji extensions and NEC selection characters
- Uses language character prefixes in MIC to identify different character sets
- Returns the number of source bytes processed
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:160-298
- Implements comprehensive SJIS to MIC mapping with error handling for malformed sequences

## Simplified Source

```c
static int sjis2mic(const unsigned char *sjis, unsigned char *p, int len, bool noError) {
    const unsigned char *start = sjis;
    int c1, c2, i, k, k2;

    while (len > 0) {
        c1 = *sjis;

        // Handle JIS X0201 katakana (single-byte, 0xA1-0xDF)
        if (c1 >= 0xa1 && c1 <= 0xdf) {
            *p++ = LC_JISX0201K;    // MIC language code for katakana
            *p++ = c1;
            sjis++;
            len--;
        }
        // Handle multi-byte Japanese characters
        else if (IS_HIGHBIT_SET(c1)) {
            // Validate 2-byte SJIS sequence
            if (len < 2 || !ISSJISHEAD(c1) || !ISSJISTAIL(sjis[1])) {
                if (!noError) {
                    report_invalid_encoding(PG_SJIS, (const char *) sjis, len);
                }
                break;
            }

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
                *p++ = LC_JISX0208;
                *p++ = ((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e);
                *p++ = c2 + ((c2 > 0x9e) ? 2 : 0x60) + (c2 < 0x80);
            }
            else if ((k >= 0xeb40 && k < 0xf040) || (k >= 0xfc4c && k <= 0xfcfc)) {
                // NEC/IBM extension characters - use alternative code
                *p++ = LC_JISX0208;
                *p++ = PGEUCALTCODE >> 8;
                *p++ = PGEUCALTCODE & 0xff;
            }
            else if (k >= 0xf040 && k < 0xf540) {
                // User-defined characters 1 (UDC1) -> JIS X0208
                *p++ = LC_JISX0208;
                c1 -= 0x6f;
                *p++ = ((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e);
                *p++ = c2 + ((c2 > 0x9e) ? 2 : 0x60) + (c2 < 0x80);
            }
            else if (k >= 0xf540 && k < 0xfa40) {
                // User-defined characters 2 (UDC2) -> JIS X0212
                *p++ = LC_JISX0212;
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
                            *p++ = LC_JISX0212;
                            *p++ = 0x80 | ((k & 0xff00) >> 8);
                            *p++ = 0x80 | (k & 0xff);
                        } else {
                            *p++ = LC_JISX0208;
                            *p++ = 0x80 | (k >> 8);
                            *p++ = 0x80 | (k & 0xff);
                        }
                        break;
                    }
                }
            }

            sjis += 2;
            len -= 2;
        }
        // Handle ASCII characters
        else {
            if (c1 == 0) {
                if (!noError) {
                    report_invalid_encoding(PG_SJIS, (const char *) sjis, len);
                }
                break;
            }
            *p++ = c1;
            sjis++;
            len--;
        }
    }

    *p = '\0';
    return sjis - start;
}
```