# mic2euc_jp

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:467-533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L467-L533)

## Overview
Core conversion function that transforms PostgreSQL's Mule Internal Code (MIC) encoded text to Japanese EUC-JP (Extended Unix Code for Japanese) encoding, implementing the reverse of euc_jp2mic conversion.

## Definition

```c
static int
mic2euc_jp(const unsigned char *mic, unsigned char *p, int len, bool noError)
```
## Detailed Description
This function converts MIC encoded Japanese text back to EUC-JP format by processing MIC language character (LC) prefixes and generating the appropriate EUC-JP byte sequences. It handles the conversion of different Japanese character sets: JIS X0201 katakana (LC_JISX0201K) becomes SS2 + character byte, JIS X0212 supplementary kanji (LC_JISX0212) becomes SS3 + two character bytes, and JIS X0208 kanji/kana (LC_JISX0208) becomes two character bytes directly. ASCII characters are copied unchanged.

## Parameters / Member Variables
- `*mic`: Source string in Mule Internal Code encoding to be converted
- `*p`: Destination buffer where EUC-JP encoded output will be written
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
  - SS2: Single Shift 2 byte (0x8E) for JIS X0201 katakana in EUC-JP
  - SS3: Single Shift 3 byte (0x8F) for JIS X0212 characters in EUC-JP
  - PG_MULE_INTERNAL: PostgreSQL encoding constant for MIC
  - PG_EUC_JP: PostgreSQL encoding constant for EUC-JP
- Called from (representative examples):
  - [mic_to_euc_jp](mic_to_euc_jp.md): PostgreSQL function wrapper for MIC to EUC-JP conversion
  - PGEUCALTCODE: Referenced in encoding conversion system

## Notes and Other Information
- Handles ASCII characters (0x00-0x7F) by direct copying
- Converts MIC language character prefixes back to EUC-JP shift sequences
- Maps LC_JISX0201K to SS2 + 1 byte sequence in EUC-JP
- Maps LC_JISX0212 to SS3 + 2 byte sequence in EUC-JP  
- Maps LC_JISX0208 directly to 2 byte sequence (no shift byte needed)
- Reports untranslatable characters for unknown language character codes
- Validates MIC character sequences using pg_encoding_verifymbchar
- Returns the number of source bytes processed
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:467-533
- Implements clean reverse mapping from MIC to EUC-JP structure

## Simplified Source

```c
static int mic2euc_jp(const unsigned char *mic, unsigned char *p, int len, bool noError) {
    const unsigned char *start = mic;
    int c1, l;

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

        // Convert based on MIC language character codes
        if (c1 == LC_JISX0201K) {
            // JIS X0201 katakana -> SS2 + 1 byte
            *p++ = SS2;
            *p++ = mic[1];
        }
        else if (c1 == LC_JISX0212) {
            // JIS X0212 supplementary kanji -> SS3 + 2 bytes
            *p++ = SS3;
            *p++ = mic[1];
            *p++ = mic[2];
        }
        else if (c1 == LC_JISX0208) {
            // JIS X0208 kanji/kana -> 2 bytes directly
            *p++ = mic[1];
            *p++ = mic[2];
        }
        else {
            // Unknown language character code
            if (!noError) {
                report_untranslatable_char(PG_MULE_INTERNAL, PG_EUC_JP,
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