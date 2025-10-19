# pg_euckr_verifychar

## Location
[src/common/wchar.c:1166-1194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1166-L1194)

## Overview
Validates a single multibyte character in EUC-KR (Extended Unix Code for Korean) encoding by checking character sequence validity and returning the character length in bytes.

## Definition
static int pg_euckr_verifychar(const unsigned char *s, int len)

## Detailed Description
This function validates EUC-KR encoded characters according to the Korean character encoding standard. EUC-KR is a variable-width encoding that represents characters using either 1 or 2 bytes:

- **ASCII characters** (0x00-0x7F): Single byte, characters without high bit set
- **Korean characters**: Two bytes, both in the valid EUC range (0xA1-0xFE)

The function examines the first byte to determine if it's a single-byte ASCII character or the start of a two-byte Korean character sequence. For two-byte sequences, it validates that both bytes fall within the valid EUC range. The function returns the character length in bytes if the sequence is valid, or -1 if it's invalid or incomplete.

## Parameters / Member Variables
- : Pointer to the first byte of the character sequence to verify
- : Remaining length of the string buffer

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro checking if high bit is set)
  - IS_EUC_RANGE_VALID (macro checking if byte is in range 0xA1-0xFE)
- Called from (representative examples):
  - [pg_euckr_verifystr](pg_euckr_verifystr.md)
  - pg_euccn_verifychar (reused for Chinese validation)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (indirectly through function pointer tables)

## Notes and Other Information
- EUC-KR is simpler than EUC-JP as it only has two character types (ASCII and Korean)
- The function is also reused by pg_euccn_verifychar for Chinese character validation since both EUC-KR and EUC-CN share the same basic structure
- Korean characters always use exactly 2 bytes in EUC-KR encoding
- The IS_EUC_RANGE_VALID macro ensures both bytes of Korean characters are in the valid range (0xA1-0xFE)
- Returns -1 immediately if the required character length exceeds available buffer length
- More straightforward than EUC-JP validation since there are no shift sequences (SS2/SS3) or three-byte characters

## Simplified Source

```c
static int pg_euckr_verifychar(const unsigned char *s, int len) {
    unsigned char c1 = *s;

    if (IS_HIGHBIT_SET(c1)) {
        // Korean character: 2 bytes, both in EUC range
        if (len < 2) return -1;
        if (!IS_EUC_RANGE_VALID(c1) || !IS_EUC_RANGE_VALID(s[1])) return -1;
        return 2;
    } else {
        // ASCII character: 1 byte
        return 1;
    }
}
```