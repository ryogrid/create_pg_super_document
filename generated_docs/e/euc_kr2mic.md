# euc_kr2mic

## Location
src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c: 76 - 123

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
  - pg_encoding_verifymbchar (validates multi-byte character sequences)
  - report_invalid_encoding (reports encoding validation errors)
- Constants referenced:
  - PG_EUC_KR (EUC-KR encoding identifier)
  - LC_KS5601 (MULE language code for Korean KS5601 character set)
- Called from:
  - euc_kr_to_mic (PostgreSQL wrapper function)

## Notes and Other Information
- This is a static (internal) function that implements the actual conversion logic
- EUC-KR characters are 2-byte sequences where both bytes have the high bit set
- In MULE format, Korean characters are prefixed with LC_KS5601 language identifier
- The function null-terminates the output string
- Returns the number of input bytes processed
- Validates character boundaries using PostgreSQL's encoding verification system
- Handles embedded null bytes as invalid input in EUC-KR context
- Part of PostgreSQL's comprehensive multi-byte character encoding conversion infrastructure