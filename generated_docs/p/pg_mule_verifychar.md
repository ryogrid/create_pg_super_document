# pg_mule_verifychar

## Location
[src/common/wchar.c:1360-1380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1360-L1380)

## Overview
Verifies the validity of a single multi-byte character in MULE (Multi-lingual Emacs) encoding by checking that all continuation bytes have the high bit set.

## Definition

```c
static int
pg_mule_verifychar(const unsigned char *s, int len)
```
## Detailed Description
This function validates a single MULE-encoded character by performing two key checks:
1. Determines the expected length of the multi-byte character using 
2. Verifies that all continuation bytes (bytes after the first) have the high bit set using 

The function ensures that multi-byte MULE characters follow the proper encoding rules where continuation bytes must have their most significant bit set to 1. This is a fundamental requirement of the MULE encoding scheme to distinguish continuation bytes from single-byte ASCII characters.

## Parameters / Member Variables
- `*s`: Pointer to the beginning of the character sequence to verify
- `len`: Maximum number of bytes available in the input buffer
## Dependencies
- Functions called/Symbols referenced:
  - [pg_mule_mblen](pg_mule_mblen.md)
  - IS_HIGHBIT_SET
- Called from (representative examples):
  - [pg_mule_verifystr](pg_mule_verifystr.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the length of the valid multi-byte character if successful, or -1 if invalid
- Part of PostgreSQL's character encoding validation infrastructure for MULE encoding
- The function is static, indicating it's only used within the wchar.c compilation unit
- MULE encoding was historically used by Emacs for multi-lingual text support

## Simplified Source

```c
static int
pg_mule_verifychar(const unsigned char *s, int len)
{
    // Get expected character length using MULE byte length function
    int char_len = pg_mule_mblen(s);

    // Check if we have enough bytes available
    if (len < char_len)
        return -1;

    // Verify all continuation bytes have high bit set
    for (int i = 1; i < char_len; i++) {
        unsigned char c = s[i];
        if (!IS_HIGHBIT_SET(c))
            return -1;  // Invalid continuation byte
    }

    return char_len;  // Valid character
}
```