# pg_gb18030_mblen

## Location
[src/common/wchar.c:1015-1028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L1015-L1028)

## Overview
Returns the byte length of a GB18030-encoded character, supporting both 2-byte and 4-byte Chinese character sequences.

## Definition
```c
static int pg_gb18030_mblen(const unsigned char *s)
```

## Detailed Description
This function determines the byte length of a character in GB18030 encoding, which is a Chinese character encoding standard that extends GBK and GB2312. GB18030 is unique among the encodings handled by PostgreSQL because it supports variable-length multi-byte characters: 1 byte for ASCII, 2 bytes for most Chinese characters, and 4 bytes for certain Unicode characters.

The function implements a sophisticated detection algorithm: ASCII characters (high bit not set) are 1 byte, characters where the second byte is a digit (0x30-0x39) are 4-byte sequences, and all other multi-byte characters are 2 bytes. This detection logic works even when only the first byte is available, as noted in the extensive comments - a 4-byte character will be reported as two 2-byte characters, which is sufficient for client-only encoding usage.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the character to examine. The function may also examine the second byte (s+1) to distinguish between 2-byte and 4-byte sequences.

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
- Called from (representative examples):
  - pg_encoding_set_invalid (indirectly through encoding function tables)

## Notes and Other Information
- This is a static function within src/common/wchar.c, used internally by PostgreSQLs character encoding subsystem
- Added by Bill Huang <bhuang@redhat.com>,<bill_huanghb@ybb.ne.jp> as noted in the source comments
- Unlike other mblen() functions in PostgreSQL, this function examines the second byte to distinguish between 2-byte and 4-byte sequences
- GB18030 is currently used as a client-only encoding in PostgreSQL - its not supported for server-side storage
- The function gracefully handles incomplete input: if only the first byte is provided, 4-byte characters are reported as two 2-byte characters, which works for current usage patterns
- GB18030 encoding structure: 1 byte (ASCII), 2 bytes (most Chinese characters), 4 bytes (extended Unicode characters)
- The detection logic for 4-byte sequences relies on the second byte being a digit (0x30-0x39), which is part of the GB18030 encoding specification