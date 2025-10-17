# big52euc_tw

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:227-307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L227-L307)

## Overview
A core conversion function that transforms text from Big5 encoding to EUC-TW (Extended Unix Code for Taiwan) encoding, handling multi-byte character sequences and generating appropriate CNS 11643 plane sequences.

## Definition
```c
static int big52euc_tw(const unsigned char *big5, unsigned char *p, int len, bool noError)
```

## Detailed Description
The `big52euc_tw` function performs the reverse conversion of `euc_tw2big5`, converting Big5 encoded text to EUC-TW format. It processes Big5 multi-byte characters, converts them to their CNS 11643 equivalents using lookup tables, and generates the appropriate EUC-TW sequences including SS2 escape sequences for characters in planes beyond CNS 11643-1. The function handles different CNS character planes and produces the correct EUC-TW multi-byte sequences based on the target plane.

## Parameters / Member Variables
- `big5`: Pointer to the source string in Big5 encoding
- `p`: Pointer to the destination buffer for EUC-TW encoded output
- `len`: Length of the source string in bytes  
- `noError`: Boolean flag - if true, stops conversion on error; if false, reports errors and continues

## Dependencies
- Functions called/Symbols referenced:
  - `IS_HIGHBIT_SET`: Macro to check if high bit is set in a byte
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md): Verify multibyte character validity for PG_BIG5
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report invalid encoding sequences
  - [report_untranslatable_char](../r/report_untranslatable_char.md): Report characters that cannot be converted
  - [BIG5toCNS](../B/BIG5toCNS.md): Convert Big5 character codes to CNS 11643 equivalents and determine plane
  - `SS2`: Single Shift 2 character constant for plane switching
  - `LC_CNS11643_1`, `LC_CNS11643_2`, `LC_CNS11643_3`, `LC_CNS11643_7`: CNS character plane constants
  - `PG_BIG5`, `PG_EUC_TW`: Encoding identifier constants
- Called from:
  - [big5_to_euc_tw](big5_to_euc_tw.md): Main wrapper function for Big5 to EUC-TW conversion

## Notes and Other Information
- Handles ASCII characters (single-byte) by passing them through unchanged
- For CNS 11643-1 characters: outputs direct 2-byte EUC-TW sequences
- For CNS 11643-2 characters: outputs SS2 + 0xa2 + 2-byte CNS code (4 bytes total)
- For CNS 11643-3 through 11643-7: outputs SS2 + plane identifier + 2-byte CNS code
- Validates Big5 multibyte character boundaries using PostgreSQL's encoding verification
- Returns the number of bytes processed from the input
- Null-terminates the output buffer
- Complementary function to `euc_tw2big5` for bidirectional conversion support

## Simplified Source

```c
static int big52euc_tw(const unsigned char *big5, unsigned char *p, int len, bool noError) {
    const unsigned char *start = big5;

    while (len > 0) {
        unsigned short c1 = *big5;

        if (IS_HIGHBIT_SET(c1)) {
            // Verify multi-byte character in Big5
            int char_len = pg_encoding_verifymbchar(PG_BIG5, (const char *) big5, len);
            if (char_len < 0) {
                if (noError) break;
                report_invalid_encoding(PG_BIG5, (const char *) big5, len);
            }

            // Convert Big5 to CNS format
            unsigned short big5buf = (c1 << 8) | big5[1];
            unsigned char plane;
            unsigned short cnsBuf = BIG5toCNS(big5buf, &plane);

            // Generate appropriate EUC-TW sequence based on plane
            if (plane == LC_CNS11643_1) {
                // Direct 2-byte output
                *p++ = (cnsBuf >> 8) & 0x00ff;
                *p++ = cnsBuf & 0x00ff;
            } else if (plane == LC_CNS11643_2) {
                // SS2 + 0xa2 + 2-byte CNS code
                *p++ = SS2;
                *p++ = 0xa2;
                *p++ = (cnsBuf >> 8) & 0x00ff;
                *p++ = cnsBuf & 0x00ff;
            } else if (plane >= LC_CNS11643_3 && plane <= LC_CNS11643_7) {
                // SS2 + plane identifier + 2-byte CNS code
                *p++ = SS2;
                *p++ = plane - LC_CNS11643_3 + 0xa3;
                *p++ = (cnsBuf >> 8) & 0x00ff;
                *p++ = cnsBuf & 0x00ff;
            } else {
                if (noError) break;
                report_untranslatable_char(PG_BIG5, PG_EUC_TW, (const char *) big5, len);
            }

            big5 += char_len;
            len -= char_len;
        } else {
            // ASCII character - copy directly
            if (c1 == 0) {
                if (noError) break;
                report_invalid_encoding(PG_BIG5, (const char *) big5, len);
            }
            *p++ = c1;
            big5++;
            len--;
        }
    }

    *p = '\0';
    return big5 - start;
}
```