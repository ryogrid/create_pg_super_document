# pg_ascii_mblen

## Location
[src/common/wchar.c:85-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L85-L90)

## Overview
Returns the byte length of an ASCII character, which is always 1 byte for the ASCII encoding.

## Definition
```c
static int pg_ascii_mblen(const unsigned char *s)
```

## Detailed Description
This function implements the multi-byte character length determination interface for ASCII encoding. Since ASCII is a single-byte encoding where every character occupies exactly one byte, this function always returns 1 regardless of the input character. 

The function is part of PostgreSQL's multi-byte encoding support framework, where different encodings provide their own mblen() implementations. For ASCII, this is the simplest possible implementation since there are no multi-byte characters to consider.

## Parameters / Member Variables
- `s`: Pointer to the first byte of the character (unused in ASCII since all characters are single-byte)

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple constant return)
- Called from (representative examples):
  - pg_encoding_set_invalid

## Notes and Other Information
- This is a static function internal to the wchar.c module
- Part of PostgreSQL's encoding abstraction layer that allows uniform handling of different character encodings
- The parameter is not actually examined since ASCII characters are always 1 byte
- Used as a function pointer in encoding tables to provide consistent interface across different encodings
- According to the file comments, mblen() functions generally only need to examine the first byte to determine length, and ASCII is the simplest case where this is always 1