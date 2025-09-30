# pg_utf_mblen

## Location
[src/common/wchar.c:538-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L538-L572)

## Overview
Returns the byte length of a UTF-8 character sequence pointed to by the given byte pointer.

## Definition
```c
int pg_utf_mblen(const unsigned char *s)
```

## Detailed Description
This function determines the number of bytes that comprise a single UTF-8 character by examining the leading byte's bit pattern. It implements the standard UTF-8 encoding rules for determining character length based on the prefix bits:

- Characters with leading bit 0 (0xxxxxxx): 1-byte ASCII characters
- Characters with leading bits 110 (110xxxxx): 2-byte UTF-8 sequences  
- Characters with leading bits 1110 (1110xxxx): 3-byte UTF-8 sequences
- Characters with leading bits 11110 (11110xxx): 4-byte UTF-8 sequences

The current PostgreSQL implementation intentionally limits support to a maximum of 4 bytes per UTF-8 character, which covers the entire Unicode range (U+0000 to U+10FFFF). The function includes commented-out code for 5-byte and 6-byte sequences, which are not part of the standard UTF-8 specification.

For invalid or unsupported leading bytes, the function conservatively returns 1, treating them as single-byte characters to avoid parsing errors.

## Parameters / Member Variables
- `s`: Pointer to the unsigned character byte that begins a UTF-8 character sequence

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only bitwise operations)
- Called from (representative examples):
  - [unicode_assigned](../u/unicode_assigned.md)
  - [unicode_normalize_func](../u/unicode_normalize_func.md)
  - [UtfToLocal](../U/UtfToLocal.md)
  - [pg_unicode_to_server](pg_unicode_to_server.md)
  - [pg_wchar2utf_with_len](pg_wchar2utf_with_len.md)
  - [pg_saslprep](pg_saslprep.md)
  - STRIDE_LENGTH

## Notes and Other Information
- This is a non-static function with external linkage, accessible from other translation units
- The function does not validate the correctness of the UTF-8 sequence beyond the leading byte pattern
- Returns a maximum value of 4 bytes, consistent with PostgreSQL's UTF-8 implementation limits
- Used extensively throughout PostgreSQL's Unicode and character encoding processing infrastructure
- The implementation deliberately avoids supporting UTF-8 sequences longer than 4 bytes, as these are not needed for standard Unicode character representation
- Critical for proper UTF-8 string processing, character boundary detection, and memory allocation calculations

## Simplified Source

```c
int pg_utf_mblen(const unsigned char *s) {
    // Determine UTF-8 character length from leading byte bit pattern

    // 1-byte: 0xxxxxxx (ASCII)
    if ((*s & 0x80) == 0)
        return 1;

    // 2-byte: 110xxxxx
    else if ((*s & 0xe0) == 0xc0)
        return 2;

    // 3-byte: 1110xxxx
    else if ((*s & 0xf0) == 0xe0)
        return 3;

    // 4-byte: 11110xxx
    else if ((*s & 0xf8) == 0xf0)
        return 4;

    // Invalid or unsupported leading byte - treat as single byte
    else
        return 1;
}
```