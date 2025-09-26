# pg_euccn_dsplen

## Location
[src/common/wchar.c:283-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L283-L298)

## Overview
A static function that determines the display width of a character sequence in EUC-CN (Extended Unix Code for Chinese) encoding.

## Definition
```c
static int pg_euccn_dsplen(const unsigned char *s)
```

## Detailed Description
The pg_euccn_dsplen function calculates how many display columns a character sequence will occupy when rendered on screen in EUC-CN encoding. It uses a simplified approach specifically optimized for EUC-CN: characters with the high bit set (Chinese characters) always occupy 2 display columns, while ASCII characters delegate to the standard ASCII display length function for proper handling of control characters and special cases.

## Parameters / Member Variables
- `s`: Pointer to the first byte of a character sequence in EUC-CN encoding

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to test if the high bit of a byte is set)
  - pg_ascii_dsplen (function to handle ASCII character display width)
- Called from (representative examples):
  - pg_encoding_set_invalid (via function pointer assignment)

## Notes and Other Information
- This is a static function with internal linkage, only visible within the wchar.c compilation unit
- Returns 2 for all Chinese characters (high-bit set), indicating double-width display
- Delegates ASCII character handling to pg_ascii_dsplen, which returns 1 for printable ASCII, 0 for null, and -1 for control characters
- Uses simplified logic compared to the generic EUC display length function since EUC-CN primarily uses 1-2 byte characters
- The function design reflects the visual properties of Chinese characters, which typically require double the horizontal space of ASCII characters when displayed
- More efficient than the generic EUC handler by avoiding checks for SS2/SS3 prefixes that are uncommon in EUC-CN