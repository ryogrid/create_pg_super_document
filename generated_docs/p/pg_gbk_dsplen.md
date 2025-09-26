# pg_gbk_dsplen

## Location
[src/common/wchar.c:961-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L961-L975)

## Overview
Returns the display length (width) of a GBK-encoded character for proper terminal display formatting.

## Definition
```c
static int pg_gbk_dsplen(const unsigned char *s)
```

## Detailed Description
This function determines the display width of a GBK (GuoBiao Kuozhan) encoded character. GBK is a character encoding standard for Simplified Chinese characters that extends GB2312. The function uses a simple heuristic: if the first byte has the high bit set (indicating a multi-byte character), it assumes the character is 2 bytes wide and takes 2 display columns. Otherwise, it delegates to `pg_ascii_dsplen` to handle ASCII characters.

The function is part of PostgreSQLs character encoding support system, specifically handling display width calculations for GBK-encoded text, which is crucial for proper formatting in terminal output, alignment in psql, and other display-related operations.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the character to examine. Must point to a valid GBK-encoded character sequence.

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - pg_ascii_dsplen (handles ASCII character display length)
- Called from (representative examples):
  - pg_encoding_set_invalid (indirectly through encoding function tables)

## Notes and Other Information
- This is a static function within src/common/wchar.c, so its only used internally within the character encoding subsystem
- The function uses a simplified approach that assumes all multi-byte GBK characters take exactly 2 display columns, which is generally correct for most Chinese characters
- GBK encoding uses 1 byte for ASCII characters (0x00-0x7F) and 2 bytes for Chinese characters and other symbols
- The function is registered as part of PostgreSQLs encoding function dispatch system for the GBK encoding