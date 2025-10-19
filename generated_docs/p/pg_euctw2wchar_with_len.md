# pg_euctw2wchar_with_len

## Location
[src/common/wchar.c:299-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L299-L338)

## Overview
Converts EUC-TW (Extended Unix Code for Taiwan) encoded multibyte string to PostgreSQL wide character representation with specified length limit.

## Definition

```c
static int
pg_euctw2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
```
## Detailed Description
This function performs character encoding conversion from EUC-TW to PostgreSQL's internal wide character format (pg_wchar). EUC-TW is a variable-length multibyte encoding system used for Traditional Chinese characters in Taiwan. The function processes up to  bytes from the input string and handles different code sets within EUC-TW:

- **Code Set 0**: Single-byte ASCII characters (0x00-0x7F)
- **Code Set 1**: Two-byte characters with high bit set
- **Code Set 2**: Four-byte sequences starting with SS2 (Single Shift 2)
- **Code Set 3**: Three-byte sequences starting with SS3 (Single Shift 3, marked as unused)

The conversion preserves the multibyte structure by encoding different code sets into different bit patterns within the pg_wchar value, allowing for proper round-trip conversion.

## Parameters / Member Variables
- `*from`: Pointer to the input EUC-TW encoded byte string to convert
- `*to`: Pointer to the output buffer where converted pg_wchar characters will be stored
- `len`: Maximum number of input bytes to process from the source string
## Dependencies
- Functions called/Symbols referenced:
  - SS2 (Single Shift 2 control character constant)
  - SS3 (Single Shift 3 control character constant)
  - IS_HIGHBIT_SET (macro to check if high bit is set in byte)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (indirectly through encoding conversion tables)

## Notes and Other Information
- Returns the number of wide characters produced (not including null terminator)
- Always null-terminates the output string
- Handles variable-length character sequences (1-4 bytes per character)
- Code Set 3 is marked as unused but still supported for completeness
- The function encodes code set information in high-order bits of pg_wchar for proper identification during reverse conversion
- Part of PostgreSQL's comprehensive multibyte character encoding support system

## Simplified Source

```c
static int pg_euctw2wchar_with_len(const unsigned char *from, pg_wchar *to, int len) {
    int cnt = 0;

    // Process each character in the input buffer
    while (len > 0 && *from) {
        if (*from == SS2 && len >= 4) {
            // Code set 2: 4-byte sequence (plane 1-16 characters)
            from++;
            *to = (((uint32) SS2) << 24) | (*from++ << 16) | (*from++ << 8) | *from++;
            len -= 4;
        } else if (*from == SS3 && len >= 3) {
            // Code set 3: 3-byte sequence (unused in practice)
            from++;
            *to = (SS3 << 16) | (*from++ << 8) | *from++;
            len -= 3;
        } else if (IS_HIGHBIT_SET(*from) && len >= 2) {
            // Code set 1: 2-byte Traditional Chinese characters
            *to = (*from++ << 8) | *from++;
            len -= 2;
        } else {
            // ASCII: 1-byte characters
            *to = *from++;
            len--;
        }
        to++;
        cnt++;
    }

    *to = 0;  // Null terminate
    return cnt;  // Return number of wide chars produced
}
```