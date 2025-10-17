# mic2euc_kr

## Location
[src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c:124-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c#L124-L174)

## Overview
Performs the actual character-by-character conversion from PostgreSQL's internal MULE encoding to EUC-KR encoding, handling Korean characters with language prefixes and ASCII characters.

## Definition
```c
static int mic2euc_kr(const unsigned char *mic, unsigned char *p, int len, bool noError)
```

## Detailed Description
This static function implements the core logic for converting MULE (Multi-byte Universal Language Environment) encoded text back to EUC-KR format. The function processes the input byte stream, identifying ASCII characters (which are copied directly) and MULE-encoded Korean characters (which are prefixed with LC_KS5601 language identifier). For Korean characters, it strips the MULE language prefix and copies the original 2-byte EUC-KR character sequence. The function includes comprehensive error handling for invalid byte sequences, untranslatable characters, and supports a no-error mode that stops conversion upon encountering problematic input.

## Parameters / Member Variables
- `mic`: Pointer to the source string in MULE encoding to be converted
- `p`: Pointer to the destination buffer where the EUC-KR-encoded result will be stored
- `len`: Length of the source string in bytes to process
- `noError`: Boolean flag indicating whether to stop silently on invalid input (true) or report errors (false)

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md) (validates multi-byte character sequences)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (reports encoding validation errors)
  - [report_untranslatable_char](../r/report_untranslatable_char.md) (reports untranslatable character errors)
- Constants referenced:
  - PG_MULE_INTERNAL (MULE internal encoding identifier)
  - PG_EUC_KR (EUC-KR encoding identifier)
  - LC_KS5601 (MULE language code for Korean KS5601 character set)
- Called from:
  - [mic_to_euc_kr](mic_to_euc_kr.md) (PostgreSQL wrapper function)

## Notes and Other Information
- This is a static (internal) function that implements the actual reverse conversion logic
- ASCII characters (without high bit set) are copied directly to output
- Korean characters in MULE format are identified by the LC_KS5601 language prefix
- Only LC_KS5601-prefixed characters are translatable to EUC-KR; other language codes trigger untranslatable character errors
- The function null-terminates the output string
- Returns the number of input bytes processed
- Handles embedded null bytes as invalid input in MULE context
- More complex than euc_kr2mic due to the need to handle multiple language codes in MULE format
- Part of PostgreSQL's bidirectional multi-byte character encoding conversion system

## Simplified Source

```c
static int mic2euc_kr(const unsigned char *mic, unsigned char *p, int len, bool noError) {
    const unsigned char *start = mic;

    while (len > 0) {
        int c1 = *mic;

        // Handle ASCII characters (copy directly)
        if (!IS_HIGHBIT_SET(c1)) {
            if (c1 == 0) {
                if (noError) break;
                report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
            }
            *p++ = c1;
            mic++;
            len--;
            continue;
        }

        // Verify multi-byte character in MULE encoding
        int char_len = pg_encoding_verifymbchar(PG_MULE_INTERNAL, (const char *) mic, len);
        if (char_len < 0) {
            if (noError) break;
            report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
        }

        // Convert Korean characters (LC_KS5601 prefix)
        if (c1 == LC_KS5601) {
            // Strip MULE prefix and copy EUC-KR bytes
            *p++ = mic[1];
            *p++ = mic[2];
        } else {
            // Other language codes are not translatable to EUC-KR
            if (noError) break;
            report_untranslatable_char(PG_MULE_INTERNAL, PG_EUC_KR, (const char *) mic, len);
        }

        mic += char_len;
        len -= char_len;
    }

    *p = '\0';
    return mic - start;
}
```