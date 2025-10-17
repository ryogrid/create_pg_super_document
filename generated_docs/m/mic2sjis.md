# mic2sjis

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:299-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L299-L405)

## Overview
Core conversion function that transforms PostgreSQL's Mule Internal Code (MIC) encoded text to Japanese Shift JIS (SJIS) encoding, handling various Japanese character sets and user-defined character areas.

## Definition

```c
static int
mic2sjis(const unsigned char *mic, unsigned char *p, int len, bool noError)
```
## Detailed Description
This function performs the reverse conversion of sjis2mic, transforming MIC encoded Japanese text back to Shift JIS format. It processes MIC language character codes to identify different Japanese character sets (JIS X0201 katakana, JIS X0208 kanji/kana, JIS X0212 supplementary kanji) and converts them to their corresponding SJIS byte sequences. The function handles user-defined character areas (UDC1 and UDC2) and uses lookup tables for IBM kanji mappings. It includes proper validation of MIC character sequences and error handling for untranslatable characters.

## Parameters / Member Variables
- `*mic`: Source string in Mule Internal Code encoding to be converted
- `*p`: Destination buffer where SJIS encoded output will be written
- `len`: Length of the source MIC string in bytes
- `noError`: Boolean flag indicating whether to suppress error reporting for invalid sequences
## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET: Check if character has high bit set
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md): Validate MIC character sequence length
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report encoding conversion errors
  - [report_untranslatable_char](../r/report_untranslatable_char.md): Report characters that cannot be converted
  - LC_JISX0201K: Language character code for JIS X0201 katakana
  - LC_JISX0208: Language character code for JIS X0208
  - LC_JISX0212: Language character code for JIS X0212
  - PGSJISALTCODE: Alternative SJIS code for unmappable characters
  - ibmkanji: Lookup table for IBM kanji mappings
  - PG_MULE_INTERNAL: PostgreSQL encoding constant for MIC
  - PG_SJIS: PostgreSQL encoding constant for Shift JIS
- Called from (representative examples):
  - [mic_to_sjis](mic_to_sjis.md): PostgreSQL function wrapper for MIC to SJIS conversion
  - PGEUCALTCODE: Referenced in encoding conversion system

## Notes and Other Information
- Handles ASCII characters (0x00-0x7F) by direct copying
- Processes JIS X0201 single-byte katakana by removing LC prefix
- Converts JIS X0208 characters using algorithmic transformation with special handling for UDC1 range
- Maps JIS X0212 characters through IBM kanji lookup table or handles UDC2 range
- Uses alternative codes (PGSJISALTCODE) for characters that cannot be mapped
- Validates MIC character sequences using pg_encoding_verifymbchar
- Returns the number of source bytes processed
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:299-405
- Implements comprehensive MIC to SJIS mapping with proper error handling and validation

## Simplified Source

```c
static int mic2sjis(const unsigned char *mic, unsigned char *p, int len, bool noError) {
    const unsigned char *start = mic;
    int c1, c2, k, l;

    while (len > 0) {
        c1 = *mic;

        // Handle ASCII characters
        if (!IS_HIGHBIT_SET(c1)) {
            if (c1 == 0) {
                if (!noError) {
                    report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
                }
                break;
            }
            *p++ = c1;
            mic++;
            len--;
            continue;
        }

        // Verify MIC character sequence length
        l = pg_encoding_verifymbchar(PG_MULE_INTERNAL, (const char *) mic, len);
        if (l < 0) {
            if (!noError) {
                report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
            }
            break;
        }

        // Handle JIS X0201 katakana (2-byte MIC sequence)
        if (c1 == LC_JISX0201K) {
            *p++ = mic[1];  // Copy katakana byte directly
        }
        // Handle JIS X0208 characters (3-byte MIC sequence)
        else if (c1 == LC_JISX0208) {
            c1 = mic[1];
            c2 = mic[2];
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
        // Handle JIS X0212 characters (3-byte MIC sequence)
        else if (c1 == LC_JISX0212) {
            int i, k2;
            c1 = mic[1];
            c2 = mic[2];
            k = c1 << 8 | c2;

            // Check for user-defined characters 2 (UDC2)
            if (k >= 0xf5a1) {
                c1 -= 0x54;
                *p++ = ((c1 - 0xa1) >> 1) + ((c1 < 0xdf) ? 0x81 : 0xc1) + 0x74;
                *p++ = c2 - ((c1 & 1) ? ((c2 < 0xe0) ? 0x61 : 0x60) : 2);
            } else {
                // IBM kanji lookup
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
        // Handle unsupported MIC language codes
        else {
            if (!noError) {
                report_untranslatable_char(PG_MULE_INTERNAL, PG_SJIS,
                                         (const char *) mic, len);
            }
            break;
        }

        mic += l;
        len -= l;
    }

    *p = '\0';
    return mic - start;
}
```