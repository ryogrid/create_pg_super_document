# pg_euc_mblen

## Location
[src/common/wchar.c:144-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L144-L159)

## Overview
Determines the byte length of an EUC (Extended Unix Code) encoded character by examining its first byte to identify the character set and corresponding length.

## Definition
```c
static inline int pg_euc_mblen(const unsigned char *s)
```

## Detailed Description
This inline function efficiently determines how many bytes comprise a single EUC-encoded character by examining the first byte. EUC encoding uses different prefixes and patterns to distinguish between character sets:

- **SS2 prefix**: Indicates a 2-byte sequence (JIS X 0201 characters)
- **SS3 prefix**: Indicates a 3-byte sequence (JIS X 0212 Kanji)  
- **High bit set**: Indicates a 2-byte sequence (JIS X 0208 Kanji)
- **Low bit pattern**: Indicates a 1-byte ASCII character

This function is critical for proper character boundary detection when processing EUC-encoded text, enabling other functions to correctly parse multi-byte character sequences without splitting characters inappropriately.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the EUC character to analyze

## Dependencies
- Functions called/Symbols referenced:
  - SS2: Single Shift 2 character set selector constant
  - SS3: Single Shift 3 character set selector constant
  - IS_HIGHBIT_SET: Macro to test if the high bit (0x80) is set
- Called from (representative examples):
  - [pg_eucjp_mblen](pg_eucjp_mblen.md)
  - [pg_euckr_mblen](pg_euckr_mblen.md)  
  - [pg_johab_mblen](pg_johab_mblen.md)

## Notes and Other Information
- This is a static inline function for optimal performance, internal to the wchar.c module
- Serves as a building block for specific EUC variant implementations (EUC-JP, EUC-KR, etc.)
- Follows the general principle mentioned in file comments that mblen() functions typically only need to examine the first byte
- Essential for text processing operations that need to advance character-by-character through EUC text
- The inline qualifier suggests this function is called frequently and benefits from compiler optimization
- Part of PostgreSQL's encoding abstraction that provides uniform interfaces across different multi-byte encodings