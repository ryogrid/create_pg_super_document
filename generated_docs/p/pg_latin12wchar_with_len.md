# pg_latin12wchar_with_len

## Location
[src/common/wchar.c:839-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L839-L860)

## Overview
Converts ISO8859-1 (Latin-1) encoded characters to PostgreSQL wide character format with length limit.

## Definition
static int pg_latin12wchar_with_len(const unsigned char *from, pg_wchar *to, int len)

## Detailed Description
This function performs character conversion from ISO8859-1 (Latin-1) encoding to PostgreSQL's internal wide character representation (pg_wchar). It processes up to a specified number of characters from the source buffer, converting each byte directly to a wide character since ISO8859-1 is a single-byte encoding where each byte value corresponds to a Unicode code point. The conversion continues until either the length limit is reached or a null terminator is encountered.

## Parameters / Member Variables
- `from`: Pointer to the source buffer containing ISO8859-1 encoded characters
- `to`: Pointer to the destination buffer for converted wide characters
- `len`: Maximum number of characters to convert from the source buffer

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - direct character copying)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (multiple references for various encoding configurations)

## Notes and Other Information
- Returns the number of characters successfully converted
- Null terminates the output buffer
- Direct byte-to-wide-character mapping works for ISO8859-1 since it's compatible with Unicode for values 0-255
- The function assumes the caller has allocated sufficient space in the destination buffer
- Part of PostgreSQL's character encoding conversion system