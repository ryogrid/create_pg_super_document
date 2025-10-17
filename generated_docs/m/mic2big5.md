# mic2big5

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:511-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L511-L580)

## Overview
Converts character data from MIC (Mule Internal Code) encoding to Big5 encoding, handling multi-byte character conversion with proper CNS plane recognition and error management.

## Definition
```c
static int mic2big5(const unsigned char *mic, unsigned char *p, int len, bool noError)
```

## Detailed Description
The `mic2big5` function performs character encoding conversion from MIC (Mule Internal Code), PostgreSQL's internal multi-byte encoding format, to Big5 (traditional Chinese character encoding). The function processes MIC-encoded input, handling ASCII characters directly and converting multi-byte MIC characters through CNS 11643 intermediate representation to Big5. It recognizes different CNS planes (1, 2, and private planes 3-4 marked with LCPRV2_B) and extracts the appropriate character codes for conversion. The function includes comprehensive validation of multi-byte character boundaries and provides robust error handling for invalid or untranslatable character sequences.

## Parameters / Member Variables
- `mic`: Input buffer containing MIC encoded data to be converted
- `p`: Output buffer where the converted Big5 encoded data will be written
- `len`: Length of the input data in bytes
- `noError`: Boolean flag controlling error behavior - if true, conversion stops on errors without reporting; if false, errors are reported via PostgreSQL's error system

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro for checking if high bit is set)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting for invalid byte sequences)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md) (validates multi-byte character boundaries)
  - [CNStoBIG5](../C/CNStoBIG5.md) (converts CNS 11643 character codes to Big5 representation)
  - [report_untranslatable_char](../r/report_untranslatable_char.md) (error reporting for untranslatable characters)
  - PG_MULE_INTERNAL, PG_BIG5 (encoding constants)
  - LC_CNS11643_1, LC_CNS11643_2, LCPRV2_B (character set plane constants)
- Called from:
  - [mic_to_big5](mic_to_big5.md) (main MIC to Big5 conversion function)

## Notes and Other Information
The function implements a two-step conversion process: MIC → CNS 11643 → Big5, using the CNStoBIG5 lookup function for character mapping. ASCII characters are handled directly without conversion. The function properly distinguishes between standard CNS planes (1 and 2) and private planes (3 and 4), with private planes being identified by the LCPRV2_B prefix byte. Character extraction differs for private planes, where the plane number is in the second byte and character data starts from the third byte. The function integrates with PostgreSQL's encoding verification and error reporting systems, ensuring robust handling of malformed input. Returns the number of input bytes successfully processed, enabling proper handling of partial conversions in streaming scenarios.

## Simplified Source

```c
static int mic2big5(const unsigned char *mic, unsigned char *p, int len, bool noError) {
    const unsigned char *start = mic;

    while (len > 0) {
        unsigned short c1 = *mic;

        if (!IS_HIGHBIT_SET(c1)) {
            // ASCII character - copy directly
            if (c1 == 0) {
                if (noError) break;
                report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
            }
            *p++ = c1;
            mic++;
            len--;
            continue;
        }

        // Verify multi-byte character in MIC
        int char_len = pg_encoding_verifymbchar(PG_MULE_INTERNAL, (const char *) mic, len);
        if (char_len < 0) {
            if (noError) break;
            report_invalid_encoding(PG_MULE_INTERNAL, (const char *) mic, len);
        }

        // Process MIC character based on plane
        if (c1 == LC_CNS11643_1 || c1 == LC_CNS11643_2 || c1 == LCPRV2_B) {
            unsigned short cnsBuf;

            if (c1 == LCPRV2_B) {
                // Private plane - extract plane number and character
                c1 = mic[1];  // get plane number
                cnsBuf = (mic[2] << 8) | mic[3];
            } else {
                // Standard plane
                cnsBuf = (mic[1] << 8) | mic[2];
            }

            // Convert CNS to Big5
            unsigned short big5buf = CNStoBIG5(cnsBuf, c1);
            if (big5buf == 0) {
                if (noError) break;
                report_untranslatable_char(PG_MULE_INTERNAL, PG_BIG5, (const char *) mic, len);
            }

            *p++ = (big5buf >> 8) & 0x00ff;
            *p++ = big5buf & 0x00ff;
        } else {
            if (noError) break;
            report_untranslatable_char(PG_MULE_INTERNAL, PG_BIG5, (const char *) mic, len);
        }

        mic += char_len;
        len -= char_len;
    }

    *p = '\0';
    return mic - start;
}
```