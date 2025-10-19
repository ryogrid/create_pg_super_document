# pg_euccn2wchar_with_len

## Location
[src/common/wchar.c:232-270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L232-L270)

## Overview
A static function that converts EUC-CN (Extended Unix Code for Chinese) encoded byte sequences to wide characters (pg_wchar) with length constraints.

## Definition
```c
static int pg_euccn2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
```

## Detailed Description
The pg_euccn2wchar_with_len function converts a buffer of EUC-CN encoded bytes into an array of wide characters (pg_wchar), respecting the maximum input length constraint. EUC-CN supports multiple character sets: ASCII (1 byte), Chinese characters with high-bit set (2 bytes), and theoretically SS2/SS3 prefixed characters (3 bytes, though marked as unused). The function processes characters sequentially, converting each multi-byte sequence into a single wide character by combining the bytes into a larger integer representation.

## Parameters / Member Variables
- `from`: Pointer to the input buffer containing EUC-CN encoded bytes
- `to`: Pointer to the output buffer for storing converted wide characters  
- `len`: Maximum number of input bytes to process

## Dependencies
- Functions called/Symbols referenced:
  - SS2 (0x8e - single shift 2 prefix)
  - SS3 (0x8f - single shift 3 prefix)
  - IS_HIGHBIT_SET (macro to check if high bit is set)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (via function pointer assignment)

## Notes and Other Information
- Returns the number of wide characters produced in the output buffer
- Null-terminates the output buffer by setting the last element to 0
- Handles three character sets in EUC-CN:
  - ASCII characters (1 byte): stored directly as single wide character
  - Code set 1 (2 bytes, high-bit set): combines two bytes into one wide character
  - Code sets 2 and 3 (3 bytes, SS2/SS3 prefixed): stores prefix in upper bits, marked as unused
- The function stops processing when it reaches the length limit, encounters a null byte, or runs out of input
- This is a length-safe conversion function that prevents buffer overruns by respecting the input length constraint

## Simplified Source

```c
static int pg_euccn2wchar_with_len(const unsigned char *from, pg_wchar *to, int len) {
    int cnt = 0;

    // Process each character in the input buffer
    while (len > 0 && *from) {
        if (*from == SS2 && len >= 3) {
            // Code set 2: 3-byte sequence (unused in practice)
            from++;
            *to = (SS2 << 16) | (*from++ << 8) | *from++;
            len -= 3;
        } else if (*from == SS3 && len >= 3) {
            // Code set 3: 3-byte sequence (unused in practice)
            from++;
            *to = (SS3 << 16) | (*from++ << 8) | *from++;
            len -= 3;
        } else if (IS_HIGHBIT_SET(*from) && len >= 2) {
            // Code set 1: 2-byte Chinese characters
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