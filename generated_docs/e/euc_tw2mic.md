# euc_tw2mic

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:308-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L308-L374)

## Overview
A core conversion function that transforms text from EUC-TW (Extended Unix Code for Taiwan) encoding to MIC (Mule Internal Code) encoding, handling multi-byte character sequences and CNS 11643 character plane indicators.

## Definition
```c
static int euc_tw2mic(const unsigned char *euc, unsigned char *p, int len, bool noError)
```

## Detailed Description
The `euc_tw2mic` function converts EUC-TW encoded text to Mule Internal Code (MIC) format. It processes EUC-TW multi-byte sequences, interprets SS2 escape sequences for different CNS 11643 character planes, and generates the corresponding MIC sequences with appropriate character set indicators. For planes beyond CNS11643-2, it uses MULE private charset codes to represent the extended character planes in the MIC encoding system.

## Parameters / Member Variables
- `euc`: Pointer to the source string in EUC-TW encoding
- `p`: Pointer to the destination buffer for MIC encoded output
- `len`: Length of the source string in bytes
- `noError`: Boolean flag - if true, stops conversion on error; if false, reports errors and continues

## Dependencies
- Functions called/Symbols referenced:
  - `IS_HIGHBIT_SET`: Macro to check if high bit is set in a byte
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md): Verify multibyte character validity for PG_EUC_TW
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report invalid encoding sequences
  - `SS2`: Single Shift 2 character constant for plane switching
  - `LC_CNS11643_1`, `LC_CNS11643_2`, `LC_CNS11643_3`: CNS character plane constants  
  - `LCPRV2_B`: MULE private charset code for extended planes
  - `PG_EUC_TW`: Encoding identifier constant
- Called from:
  - [euc_tw_to_mic](euc_tw_to_mic.md): Main wrapper function for EUC-TW to MIC conversion

## Notes and Other Information
- Handles ASCII characters (single-byte) by passing them through unchanged
- For CNS 11643-1 characters: outputs LC_CNS11643_1 + 2-byte character code (3 bytes total)
- For CNS 11643-2 characters: outputs LC_CNS11643_2 + 2-byte character code via SS2 + 0xa2 sequence  
- For CNS 11643-3 and higher: outputs LCPRV2_B + plane identifier + 2-byte character code
- Validates EUC-TW multibyte character boundaries using PostgreSQL's encoding verification
- Returns the number of bytes processed from the input
- Null-terminates the output buffer
- Part of PostgreSQL's encoding conversion system, specifically for converting to MULE internal representation
- Uses MULE private charset mechanism to handle extended CNS character planes not directly supported in standard MIC

## Simplified Source

```c
static int euc_tw2mic(const unsigned char *euc, unsigned char *p, int len, bool noError) {
    const unsigned char *start = euc;

    while (len > 0) {
        int c1 = *euc;

        if (IS_HIGHBIT_SET(c1)) {
            // Verify multi-byte character in EUC-TW
            int char_len = pg_encoding_verifymbchar(PG_EUC_TW, (const char *) euc, len);
            if (char_len < 0) {
                if (noError) break;
                report_invalid_encoding(PG_EUC_TW, (const char *) euc, len);
            }

            // Handle plane switching with SS2
            if (c1 == SS2) {
                // Multi-plane character
                c1 = euc[1]; // plane number
                if (c1 == 0xa1) {
                    *p++ = LC_CNS11643_1;
                } else if (c1 == 0xa2) {
                    *p++ = LC_CNS11643_2;
                } else {
                    // Extended planes use MULE private charset
                    *p++ = LCPRV2_B;
                    *p++ = c1 - 0xa3 + LC_CNS11643_3;
                }
                *p++ = euc[2];
                *p++ = euc[3];
            } else {
                // CNS11643-1 plane
                *p++ = LC_CNS11643_1;
                *p++ = c1;
                *p++ = euc[1];
            }

            euc += char_len;
            len -= char_len;
        } else {
            // ASCII character - copy directly
            if (c1 == 0) {
                if (noError) break;
                report_invalid_encoding(PG_EUC_TW, (const char *) euc, len);
            }
            *p++ = c1;
            euc++;
            len--;
        }
    }

    *p = '\0';
    return euc - start;
}
```