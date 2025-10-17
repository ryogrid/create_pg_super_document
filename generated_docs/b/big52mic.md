# big52mic

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:446-510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L446-L510)

## Overview
Converts character data from Big5 encoding to MIC (Mule Internal Code) encoding, handling multi-byte character conversion with proper error checking and plane number management.

## Definition
```c
static int big52mic(const unsigned char *big5, unsigned char *p, int len, bool noError)
```

## Detailed Description
The `big52mic` function performs character encoding conversion from Big5 (a traditional Chinese character encoding) to MIC (Mule Internal Code), which is PostgreSQL's internal multi-byte encoding format. The function processes input byte-by-byte, handling ASCII characters directly and converting multi-byte Big5 characters through an intermediate CNS 11643 representation. Special handling is provided for private character planes 3 and 4, which require an additional leading byte (LCPRV2_B) in the MIC output. The function includes comprehensive error handling and validation, ensuring proper multi-byte character boundaries and reporting invalid or untranslatable characters when encountered.

## Parameters / Member Variables
- `big5`: Input buffer containing Big5 encoded data to be converted
- `p`: Output buffer where the converted MIC encoded data will be written
- `len`: Length of the input data in bytes
- `noError`: Boolean flag controlling error behavior - if true, conversion stops on errors without reporting; if false, errors are reported via PostgreSQL's error system

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro for checking if high bit is set)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting for invalid byte sequences)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md) (validates multi-byte character boundaries)
  - [BIG5toCNS](../B/BIG5toCNS.md) (converts Big5 character codes to CNS 11643 representation)
  - [report_untranslatable_char](../r/report_untranslatable_char.md) (error reporting for untranslatable characters)
  - PG_BIG5, PG_MULE_INTERNAL (encoding constants)
  - LC_CNS11643_3, LC_CNS11643_4, LCPRV2_B (character set plane constants)
- Called from:
  - [big5_to_mic](big5_to_mic.md) (main Big5 to MIC conversion function)

## Notes and Other Information
The function uses a two-step conversion process: Big5 → CNS 11643 → MIC, leveraging the BIG5toCNS lookup function for the character mapping. ASCII characters (with high bit unset) are copied directly without conversion. The function properly handles PostgreSQL's encoding verification system and integrates with the database's error reporting mechanisms. Private planes 3 and 4 require special LCPRV2_B prefix in MIC encoding to distinguish them from standard CNS planes. The function returns the number of input bytes successfully processed, allowing callers to handle partial conversions appropriately.

## Simplified Source

```c
static int big52mic(const unsigned char *big5, unsigned char *p, int len, bool noError) {
    const unsigned char *start = big5;

    while (len > 0) {
        unsigned short c1 = *big5;

        if (!IS_HIGHBIT_SET(c1)) {
            // ASCII character - copy directly
            if (c1 == 0) {
                if (noError) break;
                report_invalid_encoding(PG_BIG5, (const char *) big5, len);
            }
            *p++ = c1;
            big5++;
            len--;
            continue;
        }

        // Verify multi-byte character in Big5
        int char_len = pg_encoding_verifymbchar(PG_BIG5, (const char *) big5, len);
        if (char_len < 0) {
            if (noError) break;
            report_invalid_encoding(PG_BIG5, (const char *) big5, len);
        }

        // Convert Big5 to CNS and then to MIC
        unsigned short big5buf = (c1 << 8) | big5[1];
        unsigned char plane;
        unsigned short cnsBuf = BIG5toCNS(big5buf, &plane);

        if (plane != 0) {
            // Add MULE private charset prefix for planes 3 and 4
            if (plane == LC_CNS11643_3 || plane == LC_CNS11643_4)
                *p++ = LCPRV2_B;

            *p++ = plane;  // Plane number
            *p++ = (cnsBuf >> 8) & 0x00ff;
            *p++ = cnsBuf & 0x00ff;
        } else {
            if (noError) break;
            report_untranslatable_char(PG_BIG5, PG_MULE_INTERNAL, (const char *) big5, len);
        }

        big5 += char_len;
        len -= char_len;
    }

    *p = '\0';
    return big5 - start;
}
```