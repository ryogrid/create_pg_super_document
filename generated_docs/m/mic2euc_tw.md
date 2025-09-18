# mic2euc_tw

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:375-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L375-L445)

## Overview
A core conversion function that transforms text from MIC (Mule Internal Code) encoding to EUC-TW (Extended Unix Code for Taiwan) encoding, handling MIC character set indicators and generating appropriate EUC-TW sequences.

## Definition
```c
static int mic2euc_tw(const unsigned char *mic, unsigned char *p, int len, bool noError)
```

## Detailed Description
The `mic2euc_tw` function performs the reverse conversion of `euc_tw2mic`, converting MIC encoded text back to EUC-TW format. It interprets MIC character set indicators to determine the appropriate CNS 11643 character plane, then generates the corresponding EUC-TW sequences. For characters from planes beyond CNS11643-2, it processes the MULE private charset codes and generates the appropriate SS2 escape sequences with plane identifiers.

## Parameters / Member Variables
- `mic`: Pointer to the source string in MIC encoding
- `p`: Pointer to the destination buffer for EUC-TW encoded output
- `len`: Length of the source string in bytes
- `noError`: Boolean flag - if true, stops conversion on error; if false, reports errors and continues

## Dependencies
- Functions called/Symbols referenced:
  - `IS_HIGHBIT_SET`: Macro to check if high bit is set in a byte (used with negation)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md): Verify multibyte character validity for PG_MULE_INTERNAL
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report invalid encoding sequences
  - [report_untranslatable_char](../r/report_untranslatable_char.md): Report characters that cannot be converted
  - `SS2`: Single Shift 2 character constant for plane switching
  - `LC_CNS11643_1`, `LC_CNS11643_2`, `LC_CNS11643_3`, `LC_CNS11643_7`: CNS character plane constants
  - `LCPRV2_B`: MULE private charset code for extended planes
  - `PG_MULE_INTERNAL`, `PG_EUC_TW`: Encoding identifier constants
- Called from:
  - [mic_to_euc_tw](mic_to_euc_tw.md): Main wrapper function for MIC to EUC-TW conversion

## Notes and Other Information
- Handles ASCII characters (single-byte) by passing them through unchanged
- For CNS 11643-1 characters: extracts 2-byte character code from MIC sequence (removes LC indicator)
- For CNS 11643-2 characters: generates SS2 + 0xa2 + 2-byte character code sequence
- For CNS 11643-3 through 11643-7: processes MULE private charset and generates SS2 + plane ID + 2-byte code
- Validates MIC multibyte character boundaries using PostgreSQL's encoding verification
- Returns the number of bytes processed from the input
- Null-terminates the output buffer
- Complementary function to `euc_tw2mic` for bidirectional conversion support
- Handles the complexity of converting from MULE's internal representation back to standard EUC-TW format