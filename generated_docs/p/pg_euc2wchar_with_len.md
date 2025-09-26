# pg_euc2wchar_with_len

## Location
src/common/wchar.c: 105 - 143

## Overview
Converts EUC (Extended Unix Code) encoded multi-byte characters to PostgreSQL's wide character format, handling the complex multi-byte structure of EUC encodings.

## Definition
```c
static int pg_euc2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
```

## Detailed Description
This function converts EUC-encoded byte sequences to wide characters (pg_wchar), handling the variable-length nature of EUC encoding. EUC encoding supports multiple character sets within a single encoding:

1. **SS2 sequences (2 bytes)**: JIS X 0201 characters (single-byte Kana), marked with SS2 prefix
2. **SS3 sequences (3 bytes)**: JIS X 0212 Kanji characters, marked with SS3 prefix  
3. **High-bit set (2 bytes)**: JIS X 0208 Kanji characters (most common multi-byte characters)
4. **ASCII (1 byte)**: Standard 7-bit ASCII characters

The function processes input character by character, detecting the encoding type based on the first byte(s) and consuming the appropriate number of input bytes. Each logical character is converted to a single wide character value that encodes both the character set identifier and the character code.

## Parameters / Member Variables
- `from`: Source buffer containing EUC-encoded byte sequence
- `to`: Destination buffer for converted wide characters (pg_wchar)
- `len`: Maximum number of input bytes to process for buffer safety

## Dependencies
- Functions called/Symbols referenced:
  - SS2: Single Shift 2 character set selector (JIS X 0201)
  - SS3: Single Shift 3 character set selector (JIS X 0212)  
  - IS_HIGHBIT_SET: Macro to test if high bit is set (JIS X 0208 detection)
- Called from (representative examples):
  - pg_eucjp2wchar_with_len
  - pg_euckr2wchar_with_len

## Notes and Other Information
- This is a static function internal to the wchar.c module
- Used as a foundation for specific EUC variants like EUC-JP and EUC-KR
- The wide character encoding preserves the original character set information by incorporating SS2/SS3 markers into the wide character value
- Ensures proper handling of variable-length characters while respecting input buffer boundaries
- Returns count of logical characters converted (not bytes processed)
- Always null-terminates the output for safety
- Part of PostgreSQL's comprehensive multi-byte character encoding support infrastructure