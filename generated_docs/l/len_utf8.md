# len_utf8

## Location
[src/backend/snowball/libstemmer/utilities.c:478-488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L478-L488)

## Overview
Calculates the number of UTF-8 characters in a symbol string by counting UTF-8 character boundaries.

## Definition

```c
}

extern int len_utf8(const symbol * p)
```
## Detailed Description
The  function counts the number of UTF-8 characters in a symbol string pointed to by . It works by iterating through each byte of the string and identifying UTF-8 character boundaries. In UTF-8 encoding, continuation bytes (bytes that are part of a multi-byte character but not the first byte) have values between 0x80 and 0xBF, while character start bytes are either ASCII (< 0x80) or have values >= 0xC0. The function increments the character count only for bytes that represent the start of a UTF-8 character.

The function uses the  macro to determine the byte length of the symbol string, then processes each byte to distinguish between UTF-8 character start bytes and continuation bytes.

## Parameters / Member Variables
- `*p`: Pointer to a symbol string (const symbol *) whose UTF-8 character length is to be calculated
## Dependencies
- Functions called/Symbols referenced:
  -  (macro from header.h:11 - extracts size from symbol string)
  -  (typedef from api.h:2 - unsigned char type)
- Called from (representative examples):
  - Various Arabic stemmer functions in stem_UTF_8_arabic.c
  - Lithuanian stemmer function in stem_UTF_8_lithuanian.c
  - Greek and Tamil stemmer functions for minimum length checking
  - The  function through header.h:59

## Notes and Other Information
- This function is part of the Snowball stemming library used in PostgreSQL's full-text search functionality
- The function correctly handles UTF-8 multi-byte characters by counting only character start bytes
- Symbol strings in the Snowball library store their size as metadata accessible via the SIZE macro
- Used extensively in UTF-8 language stemmers to ensure minimum word lengths before applying stemming rules
- The function assumes the input string contains valid UTF-8 encoded data

## Simplified Source

```c
extern int len_utf8(const symbol * p) {
    int size = SIZE(p);  // Get byte length of string
    int len = 0;         // Character count

    // Count UTF-8 character start bytes
    while (size--) {
        symbol b = *p++;
        // Count bytes that start UTF-8 characters
        // (ASCII < 0x80 or start bytes >= 0xC0)
        if (b >= 0xC0 || b < 0x80)
            ++len;
    }

    return len;
}
```