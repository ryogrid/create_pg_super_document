# pg_gb18030_dsplen

## Location
[src/common/wchar.c:1029-1062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1029-L1062)

## Overview
Returns the display length (width) of a GB18030-encoded character for proper terminal display formatting and text alignment.

## Definition
```c
static int pg_gb18030_dsplen(const unsigned char *s)
```

## Detailed Description
This function determines the display width of a GB18030-encoded character. GB18030 is a Chinese character encoding standard that supports variable-length multi-byte characters (1, 2, or 4 bytes). However, for display purposes, this function uses a simplified approach: all multi-byte characters (those with the high bit set) are assumed to take 2 display columns, while ASCII characters are handled by the `pg_ascii_dsplen` function.

This simplified approach works well for most practical cases, as both 2-byte and 4-byte GB18030 characters typically represent Chinese characters that occupy 2 display columns in terminal output. The function is essential for proper text formatting and alignment in PostgreSQL when dealing with Chinese text encoded in GB18030.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the character to examine. Must point to a valid GB18030-encoded character sequence.

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - pg_ascii_dsplen (handles ASCII character display length)
- Called from (representative examples):
  - pg_encoding_set_invalid (indirectly through encoding function tables)

## Notes and Other Information
- This is a static function within src/common/wchar.c, used internally by PostgreSQLs character encoding subsystem
- The function uses a simplified display width calculation that treats all multi-byte GB18030 characters as 2 columns wide, regardless of whether they are 2-byte or 4-byte sequences
- This simplification is appropriate because both 2-byte and 4-byte GB18030 characters represent Chinese characters that typically occupy 2 display columns
- GB18030 is currently used as a client-only encoding in PostgreSQL
- The function is part of PostgreSQLs encoding-specific function dispatch system for uniform character encoding handling
- Proper display width calculation is crucial for features like column alignment in query results and text formatting in psql output
- Unlike pg_gb18030_mblen which distinguishes between 2-byte and 4-byte sequences, this display length function treats them uniformly for simplicity