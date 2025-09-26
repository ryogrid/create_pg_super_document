# pg_uhc_dsplen

## Location
[src/common/wchar.c:988-1014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L988-L1014)

## Overview
Returns the display length (width) of a UHC-encoded character for proper terminal display formatting and alignment.

## Definition
```c
static int pg_uhc_dsplen(const unsigned char *s)
```

## Detailed Description
This function determines the display width of a UHC (Unified Hangul Code) encoded character. UHC is a character encoding scheme used primarily for Korean text. The function follows a simple approach: if the first byte has the high bit set (indicating a multi-byte Korean character), it assumes the character takes 2 display columns; otherwise, it delegates to `pg_ascii_dsplen` to handle ASCII characters appropriately.

This function is crucial for proper text formatting and alignment in PostgreSQL when dealing with Korean text, ensuring that output in psql and other tools displays correctly by accounting for the wider display width of Korean characters.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the character to examine. Must point to a valid UHC-encoded character sequence.

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - pg_ascii_dsplen (handles ASCII character display length)
- Called from (representative examples):
  - pg_encoding_set_invalid (indirectly through encoding function tables)

## Notes and Other Information
- This is a static function within src/common/wchar.c, used internally by PostgreSQLs character encoding subsystem
- The function assumes all multi-byte UHC characters (Korean Hangul and Hanja) take exactly 2 display columns, which is generally correct for terminal and fixed-width font display
- UHC encoding uses 1 byte for ASCII characters and 2 bytes for Korean characters
- This function is part of PostgreSQLs encoding-specific function dispatch system that provides uniform handling of different character encodings
- Proper display width calculation is essential for features like column alignment in query results and formatting in psql output