# pg_latin1_dsplen

## Location
[src/common/wchar.c:882-890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L882-L890)

## Overview
Calculates the display length of a character in ISO8859-1 (Latin-1) encoding by delegating to ASCII display length calculation.

## Definition
static int pg_latin1_dsplen(const unsigned char *s)

## Detailed Description
This function determines the display width of a character in the ISO8859-1 (Latin-1) encoding by calling the ASCII display length function. Since Latin-1 is a superset of ASCII for the first 128 characters and shares the same display characteristics for control characters and printable characters, the ASCII display length calculation is appropriate for Latin-1 characters. This function serves as part of PostgreSQL's character encoding framework for determining how many screen columns a character occupies.

## Parameters / Member Variables
- `s`: Pointer to the unsigned character to analyze for display length calculation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ascii_dsplen](pg_ascii_dsplen.md) (delegated function for actual display length calculation)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (multiple references for various encoding configurations)

## Notes and Other Information
- Returns the same values as pg_ascii_dsplen: 0 for null, -1 for control characters, 1 for printable characters
- Leverages the fact that Latin-1 display characteristics match ASCII for control and printable character determination
- Part of PostgreSQL's character encoding system function pointer interface
- Provides consistent display length calculation across different single-byte encodings