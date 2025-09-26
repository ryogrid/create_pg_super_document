# pg_latin1_mblen

## Location
[src/common/wchar.c:876-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L876-L881)

## Overview
Returns the byte length of a character in ISO8859-1 (Latin-1) encoding, which is always 1 byte.

## Definition
static int pg_latin1_mblen(const unsigned char *s)

## Detailed Description
This function determines the byte length of a character in the ISO8859-1 (Latin-1) encoding. Since ISO8859-1 is a single-byte character encoding where each character is represented by exactly one byte, this function always returns 1 regardless of the input character. This function serves as part of PostgreSQL's character encoding framework, providing a consistent interface for determining character lengths across different encoding systems.

## Parameters / Member Variables
- `s`: Pointer to the character to analyze (parameter not actually used since all Latin-1 characters are single-byte)

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (multiple references for various encoding configurations)

## Notes and Other Information
- Always returns 1 since Latin-1 is a fixed single-byte encoding
- The parameter `s` is not used in the function implementation
- Part of PostgreSQL's character encoding system function pointer interface
- Provides consistency with other encoding-specific mblen functions that may have variable character lengths