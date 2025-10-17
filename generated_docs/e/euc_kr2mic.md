# euc_kr2mic

## Location
[src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c:76-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c#L76-L123)

## Overview
Performs the actual character-by-character conversion from EUC-KR encoding to PostgreSQL's internal MULE encoding, handling both multi-byte Korean characters and ASCII characters.

## Definition
```c
static int euc_kr2mic(const unsigned char *euc, unsigned char *p, int len, bool noError)
```

## Detailed Description
This static function implements the core logic for converting EUC-KR encoded text to PostgreSQL's MULE (Multi-byte Universal Language Environment) internal format. The function processes the input byte stream character by character, identifying Korean multi-byte characters (which have the high bit set) and ASCII characters. For Korean characters, it validates that they form valid 2-byte sequences and prefixes them with the LC_KS5601 language code in the MULE format. ASCII characters are copied directly. The function includes error handling to report invalid byte sequences and supports a no-error mode that stops conversion upon encountering invalid input.

## Parameters / Member Variables
- `euc`: Pointer to the source string in EUC-KR encoding to be converted
- `p`: Pointer to the destination buffer where the MULE-encoded result will be stored
- `len`: Length of the source string in bytes to process
- `noError`: Boolean flag indicating whether to stop silently on invalid input (true) or report errors (false)

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md) (validates multi-byte character sequences)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (reports encoding validation errors)
- Constants referenced:
  - PG_EUC_KR (EUC-KR encoding identifier)
  - LC_KS5601 (MULE language code for Korean KS5601 character set)
- Called from:
  - [euc_kr_to_mic](euc_kr_to_mic.md) (PostgreSQL wrapper function)

## Notes and Other Information
- This is a static (internal) function that implements the actual conversion logic
- EUC-KR characters are 2-byte sequences where both bytes have the high bit set
- In MULE format, Korean characters are prefixed with LC_KS5601 language identifier
- The function null-terminates the output string
- Returns the number of input bytes processed
- Validates character boundaries using PostgreSQL's encoding verification system
- Handles embedded null bytes as invalid input in EUC-KR context
- Part of PostgreSQL's comprehensive multi-byte character encoding conversion infrastructure

## Simplified Source

```c
static int euc_kr2mic(const unsigned char *euc, unsigned char *p, int len, bool noError) {
    const unsigned char *start = euc;
    int c1, l;

    while (len > 0) {
        c1 = *euc;

        // Handle Korean characters (high bit set)
        if (IS_HIGHBIT_SET(c1)) {
            // Verify 2-byte EUC-KR sequence
            l = pg_encoding_verifymbchar(PG_EUC_KR, (const char *) euc, len);
            if (l != 2) {
                if (!noError) {
                    report_invalid_encoding(PG_EUC_KR, (const char *) euc, len);
                }
                break;
            }

            // Convert to MIC: add language code prefix + 2 bytes
            *p++ = LC_KS5601;   // MIC language code for Korean KS5601
            *p++ = c1;          // First byte of Korean character
            *p++ = euc[1];      // Second byte of Korean character
            euc += 2;
            len -= 2;
        }
        // Handle ASCII characters
        else {
            // Check for invalid null byte
            if (c1 == 0) {
                if (!noError) {
                    report_invalid_encoding(PG_EUC_KR, (const char *) euc, len);
                }
                break;
            }

            // Copy ASCII character directly
            *p++ = c1;
            euc++;
            len--;
        }
    }

    *p = '\0';
    return euc - start;
}
```