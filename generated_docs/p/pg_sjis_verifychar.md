# pg_sjis_verifychar

## Location
[src/common/wchar.c:1427-1449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1427-L1449)

## Overview
Verifies the validity of a single Shift JIS (SJIS) encoded character by checking proper lead byte and trail byte combinations for multi-byte characters.

## Definition
```c
static int pg_sjis_verifychar(const unsigned char *s, int len)
```

## Detailed Description
This function validates a single Shift JIS encoded character by performing several checks:

1. **Length Determination**: Uses `pg_sjis_mblen()` to determine the expected character length (1 or 2 bytes)
2. **Buffer Bounds Check**: Ensures sufficient bytes are available in the input buffer
3. **Single-byte Fast Path**: For single-byte characters, relies on the validation already performed by `pg_sjis_mblen()`
4. **Multi-byte Validation**: For 2-byte characters, validates that:
   - The first byte is a valid SJIS lead byte using `ISSJISHEAD()`
   - The second byte is a valid SJIS trail byte using `ISSJISTAIL()`

Shift JIS uses a complex encoding scheme where:
- Single-byte characters cover ASCII and half-width katakana
- Double-byte characters use specific ranges for lead and trail bytes
- Not all byte combinations are valid, requiring explicit validation

## Parameters / Member Variables
- `s`: Pointer to the beginning of the character sequence to verify
- `len`: Maximum number of bytes available in the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pg_sjis_mblen](pg_sjis_mblen.md)
  - ISSJISHEAD
  - ISSJISTAIL
- Called from (representative examples):
  - [pg_sjis_verifystr](pg_sjis_verifystr.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- Returns the length of the valid character if successful, or -1 if invalid
- Part of PostgreSQL's character encoding validation infrastructure for Japanese text
- Handles both single-byte (ASCII/half-width katakana) and double-byte (full-width) characters
- The function is static, indicating it's only used within the wchar.c compilation unit
- Used for both Shift JIS and EUC_JIS_2004 encodings in PostgreSQL
- SJIS encoding is widely used in Japan for legacy systems and Windows environments

## Simplified Source

```c
static int
pg_sjis_verifychar(const unsigned char *s, int len)
{
    // Get expected character length
    int char_len = pg_sjis_mblen(s);

    // Check if we have enough bytes
    if (len < char_len)
        return -1;

    // Single-byte character - already validated by pg_sjis_mblen
    if (char_len == 1)
        return char_len;

    // Multi-byte character - validate lead and trail bytes
    unsigned char lead_byte = s[0];
    unsigned char trail_byte = s[1];

    if (!ISSJISHEAD(lead_byte) || !ISSJISTAIL(trail_byte))
        return -1;  // Invalid SJIS byte sequence

    return char_len;
}
```