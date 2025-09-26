# pg_eucjp_verifychar

## Location
src/common/wchar.c: 1082 - 1136

## Overview
Validates a single multibyte character in EUC-JP (Extended Unix Code for Japanese) encoding by checking character sequence validity and returning the character length in bytes.

## Definition
static int pg_eucjp_verifychar(const unsigned char *s, int len)

## Detailed Description
This function validates EUC-JP encoded characters according to the Japanese character encoding standard. EUC-JP is a variable-width encoding that can represent characters using 1, 2, or 3 bytes depending on the character set:

- **ASCII characters** (0x00-0x7F): Single byte, handled in default case
- **JIS X 0201 characters** (SS2 prefix): Two bytes starting with 0x8E, second byte 0xA1-0xDF
- **JIS X 0212 characters** (SS3 prefix): Three bytes starting with 0x8F, followed by two valid EUC range bytes
- **JIS X 0208 characters**: Two bytes, both in EUC valid range (0xA1-0xFE)

The function examines the first byte to determine the character type and validates the complete sequence accordingly. It returns the character length in bytes if valid, or -1 if the sequence is invalid or incomplete.

## Parameters / Member Variables
- : Pointer to the first byte of the character sequence to verify
- : Remaining length of the string buffer

## Dependencies
- Functions called/Symbols referenced:
  - SS2 (0x8E - single shift 2 for JIS X 0201)
  - SS3 (0x8F - single shift 3 for JIS X 0212)
  - IS_EUC_RANGE_VALID (macro checking if byte is in range 0xA1-0xFE)
  - IS_HIGHBIT_SET (macro checking if high bit is set)
- Called from (representative examples):
  - pg_eucjp_verifystr
  - pg_encoding_set_invalid (indirectly through function pointer tables)

## Notes and Other Information
- EUC-JP character validation requires understanding of Japanese character set structure
- The function handles three different Japanese character sets within the EUC-JP encoding
- SS2 (0x8E) introduces JIS X 0201 katakana characters (2 bytes total)
- SS3 (0x8F) introduces JIS X 0212 supplementary characters (3 bytes total)
- Characters with high bit set but not SS2/SS3 are treated as JIS X 0208 (2 bytes)
- The function properly validates byte ranges for each character set to prevent invalid sequences
- Returns -1 immediately if the required character length exceeds available buffer length