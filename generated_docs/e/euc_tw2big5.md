# euc_tw2big5

## Location
src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c: 149 - 226

## Overview
A core conversion function that transforms text from EUC-TW (Extended Unix Code for Taiwan) encoding to Big5 encoding, handling multi-byte character sequences and various CNS 11643 character planes.

## Definition
```c
static int euc_tw2big5(const unsigned char *euc, unsigned char *p, int len, bool noError)
```

## Detailed Description
The `euc_tw2big5` function performs the actual character-by-character conversion from EUC-TW encoding to Big5 encoding. It processes multi-byte sequences, handles different CNS 11643 character planes (including plane switching via SS2 sequences), and converts them to their Big5 equivalents using lookup tables. The function includes comprehensive error handling for invalid sequences and untranslatable characters, with the ability to continue processing or halt on errors based on the `noError` parameter.

## Parameters / Member Variables
- `euc`: Pointer to the source string in EUC-TW encoding
- `p`: Pointer to the destination buffer for Big5 encoded output  
- `len`: Length of the source string in bytes
- `noError`: Boolean flag - if true, stops conversion on error; if false, reports errors and continues

## Dependencies
- Functions called/Symbols referenced:
  - `IS_HIGHBIT_SET`: Macro to check if high bit is set in a byte
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md): Verify multibyte character validity for PG_EUC_TW
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report invalid encoding sequences
  - [report_untranslatable_char](../r/report_untranslatable_char.md): Report characters that cannot be converted
  - [CNStoBIG5](../C/CNStoBIG5.md): Convert CNS 11643 character codes to Big5 equivalents
  - `SS2`: Single Shift 2 character constant for plane switching
  - `LC_CNS11643_1`, `LC_CNS11643_2`, `LC_CNS11643_3`: CNS character plane constants
  - `PG_EUC_TW`, `PG_BIG5`: Encoding identifier constants
- Called from:
  - [euc_tw_to_big5](euc_tw_to_big5.md): Main wrapper function for EUC-TW to Big5 conversion

## Notes and Other Information
- Handles ASCII characters (single-byte) by passing them through unchanged
- Processes CNS 11643 plane 1 characters directly (2-byte sequences)
- Uses SS2 escape sequences to handle additional CNS planes (4-byte sequences)
- Validates multibyte character boundaries using PostgreSQL's encoding verification
- Returns the number of bytes processed from the input
- Null-terminates the output buffer
- Part of PostgreSQL's comprehensive encoding conversion system for Chinese character sets