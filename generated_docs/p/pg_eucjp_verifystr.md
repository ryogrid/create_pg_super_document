# pg_eucjp_verifystr

## Location
[src/common/wchar.c:1137-1165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1137-L1165)

## Overview
Validates an entire EUC-JP encoded string by checking each character sequence for validity and null bytes, returning the byte offset of the first invalid sequence.

## Definition
static int pg_eucjp_verifystr(const unsigned char *s, int len)

## Detailed Description
This function validates a complete EUC-JP encoded string by iterating through each character and verifying the encoding validity. It uses an optimized approach with a fast path for ASCII characters (characters without the high bit set) and delegates multibyte character validation to pg_eucjp_verifychar().

The function processes the string character by character:
1. For ASCII characters (high bit not set), it performs a quick null check and advances by 1 byte
2. For multibyte characters (high bit set), it calls pg_eucjp_verifychar() to validate the complete sequence
3. The function stops at the first invalid character or null byte encountered
4. Returns the number of valid bytes processed, which equals the total length if the entire string is valid

## Parameters / Member Variables
- : Pointer to the EUC-JP encoded string to verify  
- : Length of the string to verify in bytes

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - [pg_eucjp_verifychar](pg_eucjp_verifychar.md) (validates individual EUC-JP character sequences)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (indirectly through function pointer tables)

## Notes and Other Information
- Implements the verifystr contract: validates whole string and returns byte offset of first invalid character
- Uses a fast path optimization for ASCII characters to improve performance on mixed ASCII/Japanese text
- Must reject null bytes as required by the verifystr function contract
- The function processes variable-width characters correctly by advancing the pointer by the validated character length
- Returns the total number of valid bytes processed, making it easy to identify where validation failed
- Essential for ensuring data integrity in EUC-JP encoded text stored in PostgreSQL databases