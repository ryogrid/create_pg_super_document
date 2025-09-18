# charlen_to_bytelen

## Location
src/backend/utils/adt/varlena.c: 806 - 851

## Overview
Computes the number of bytes occupied by a specified number of characters in a string, handling both single-byte and multibyte character encodings.

## Definition
```c
static int charlen_to_bytelen(const char *p, int n)
```

## Detailed Description
The `charlen_to_bytelen` function is an internal utility that calculates how many bytes are needed to store a given number of characters starting from a specific position in a string. This function is crucial for PostgreSQL's text processing operations, particularly when dealing with multibyte character encodings where the relationship between character count and byte count is not 1:1.

The function implements an optimization for single-byte encodings where it can simply return the character count as the byte count. For multibyte encodings, it iterates through each character using `pg_mblen` to determine the byte length of each character and accumulates the total byte count.

It's the caller's responsibility to ensure that there are actually n characters available in the string starting from position p. The function does not perform bounds checking and does not require the string to be null-terminated.

## Parameters / Member Variables
- `p`: Pointer to the starting position in the string
- `n`: Number of characters to count bytes for

## Dependencies
- Functions called/Symbols referenced:
  - `pg_database_encoding_max_length`: Returns maximum bytes per character for current database encoding
  - `pg_mblen`: Returns the byte length of the multibyte character at a given position

- Called from (representative examples):
  - `appendStringInfoRegexpSubstr`: Used for regexp substring operations
  - `replace_text_regexp`: Used in text replacement with regular expressions

## Notes and Other Information
- This is a static internal function, not accessible from SQL
- Contains an important optimization for single-byte encodings that avoids the overhead of character-by-character processing
- The caller must ensure that n characters actually exist starting from position p
- No bounds checking is performed - this is by design for performance reasons
- Used primarily in regular expression and substring operations where precise byte positioning is required
- The function assumes valid character sequences and does not perform validation