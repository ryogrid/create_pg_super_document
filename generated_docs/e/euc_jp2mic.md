# euc_jp2mic

## Location
[src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:406-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c#L406-L466)

## Overview
Core conversion function that transforms Japanese EUC-JP (Extended Unix Code for Japanese) encoded text to PostgreSQL's Mule Internal Code (MIC) encoding, handling the EUC-JP character set structure.

## Definition

```c
static int
euc_jp2mic(const unsigned char *euc, unsigned char *p, int len, bool noError)
```
## Detailed Description
This function converts EUC-JP encoded Japanese text to Mule Internal Code format. EUC-JP uses a structured encoding scheme where different character sets are identified by specific byte patterns: ASCII characters (0x00-0x7F), JIS X0208 kanji and kana (high-bit set bytes), JIS X0201 katakana preceded by SS2 (0x8E), and JIS X0212 supplementary kanji preceded by SS3 (0x8F). The function identifies these patterns and adds appropriate MIC language character (LC) prefixes to distinguish the different Japanese character sets in the output.

## Parameters / Member Variables
- `*euc`: Source string in EUC-JP encoding to be converted
- `*p`: Destination buffer where MIC encoded output will be written
- `len`: Length of the source EUC-JP string in bytes
- `noError`: Boolean flag indicating whether to suppress error reporting for invalid sequences
## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET: Check if character has high bit set
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md): Validate EUC-JP character sequence length
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report encoding conversion errors
  - SS2: Single Shift 2 byte (0x8E) indicating JIS X0201 katakana
  - SS3: Single Shift 3 byte (0x8F) indicating JIS X0212 characters
  - LC_JISX0201K: Language character code for JIS X0201 katakana
  - LC_JISX0208: Language character code for JIS X0208
  - LC_JISX0212: Language character code for JIS X0212
  - PG_EUC_JP: PostgreSQL encoding constant for EUC-JP
- Called from (representative examples):
  - [euc_jp_to_mic](euc_jp_to_mic.md): PostgreSQL function wrapper for EUC-JP to MIC conversion
  - PGEUCALTCODE: Referenced in encoding conversion system

## Notes and Other Information
- Handles ASCII characters (0x00-0x7F) by direct copying
- Processes EUC-JP's structured encoding using shift sequences (SS2, SS3)
- Maps JIS X0201 katakana (SS2 + 1 byte) to LC_JISX0201K prefix
- Maps JIS X0212 supplementary kanji (SS3 + 2 bytes) to LC_JISX0212 prefix  
- Maps regular JIS X0208 kanji/kana (2 bytes with high bits set) to LC_JISX0208 prefix
- Validates EUC-JP character sequences using pg_encoding_verifymbchar
- Returns the number of source bytes processed
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:406-466
- Implements straightforward EUC-JP to MIC mapping following the EUC structure

## Simplified Source

```c
static int euc_jp2mic(const unsigned char *euc, unsigned char *p, int len, bool noError) {
    const unsigned char *start = euc;
    int c1, l;

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

        // Convert based on EUC-JP structure
        if (c1 == SS2) {
            // JIS X0201 katakana (SS2 + 1 byte)
            *p++ = LC_JISX0201K;
            *p++ = euc[1];
        }
        else if (c1 == SS3) {
            // JIS X0212 supplementary kanji (SS3 + 2 bytes)
            *p++ = LC_JISX0212;
            *p++ = euc[1];
            *p++ = euc[2];
        }
        else {
            // JIS X0208 kanji/kana (2 bytes with high bits)
            *p++ = LC_JISX0208;
            *p++ = c1;
            *p++ = euc[1];
        }

        euc += l;
        len -= l;
    }

    *p = '\0';
    return euc - start;
}
```