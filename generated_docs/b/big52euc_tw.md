# big52euc_tw

## Location
src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c: 227 - 307

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
  - `[pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)`: Verify multibyte character validity for PG_BIG5
  - `[report_invalid_encoding](../r/report_invalid_encoding.md)`: Report invalid encoding sequences
  - `[report_untranslatable_char](../r/report_untranslatable_char.md)`: Report characters that cannot be converted
  - `[BIG5toCNS](../B/BIG5toCNS.md)`: Convert Big5 character codes to CNS 11643 equivalents and determine plane
  - `SS2`: Single Shift 2 character constant for plane switching
  - `LC_CNS11643_1`, `LC_CNS11643_2`, `LC_CNS11643_3`, `LC_CNS11643_7`: CNS character plane constants
  - `PG_BIG5`, `PG_EUC_TW`: Encoding identifier constants
- Called from:
  - `[big5_to_euc_tw](big5_to_euc_tw.md)`: Main wrapper function for Big5 to EUC-TW conversion

## Notes and Other Information
- Handles ASCII characters (single-byte) by passing them through unchanged
- For CNS 11643-1 characters: outputs direct 2-byte EUC-TW sequences
- For CNS 11643-2 characters: outputs SS2 + 0xa2 + 2-byte CNS code (4 bytes total)
- For CNS 11643-3 through 11643-7: outputs SS2 + plane identifier + 2-byte CNS code
- Validates Big5 multibyte character boundaries using PostgreSQL's encoding verification
- Returns the number of bytes processed from the input
- Null-terminates the output buffer
- Complementary function to `euc_tw2big5` for bidirectional conversion support