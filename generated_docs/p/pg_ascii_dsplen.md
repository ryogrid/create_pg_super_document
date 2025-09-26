# pg_ascii_dsplen

## Location
[src/common/wchar.c:91-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L91-L104)

## Overview
Returns the display width of an ASCII character, handling special cases for null terminators and control characters according to Unicode display standards.

## Definition
```c
static int pg_ascii_dsplen(const unsigned char *s)
```

## Detailed Description
This function determines the display width of an ASCII character for proper terminal and console output formatting. It implements Unicode-compliant display width rules:

- Null character ('\0'): Returns 0 (zero width)
- Control characters (0x00-0x1F and 0x7F): Returns -1 (generally non-printable)
- Printable ASCII characters (0x20-0x7E): Returns 1 (standard width)

The function is crucial for PostgreSQL's psql client and other display components to properly format output, ensuring that control characters don't disrupt terminal display and that character positioning calculations are accurate.

## Parameters / Member Variables
- `s`: Pointer to the ASCII character to analyze for display width

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only character value comparisons)
- Called from (representative examples):
  - [pg_euc_dsplen](pg_euc_dsplen.md)
  - [pg_eucjp_dsplen](pg_eucjp_dsplen.md)  
  - [pg_euccn_dsplen](pg_euccn_dsplen.md)
  - [pg_euctw_dsplen](pg_euctw_dsplen.md)
  - [pg_latin1_dsplen](pg_latin1_dsplen.md)
  - [pg_sjis_dsplen](pg_sjis_dsplen.md)
  - [pg_big5_dsplen](pg_big5_dsplen.md)
  - [pg_gbk_dsplen](pg_gbk_dsplen.md)
  - [pg_uhc_dsplen](pg_uhc_dsplen.md)
  - [pg_gb18030_dsplen](pg_gb18030_dsplen.md)
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- This is a static function internal to the wchar.c module
- Widely reused by other encoding display length functions for their ASCII subset handling
- Follows Unicode standard recommendations for character display widths
- Critical for proper psql output formatting and terminal display consistency  
- Control characters (values < 0x20 and 0x7F) return -1 to indicate they should typically not advance cursor position
- The implementation ensures non-ASCII encodings can defer to ASCII rules for their ASCII-compatible characters, maintaining consistency across different encodings