# pg_wchar2single_with_len

## Location
[src/common/wchar.c:861-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L861-L875)

## Overview
Converts PostgreSQL wide characters to single-byte encoding by truncating high bits with length limit.

## Definition
static int pg_wchar2single_with_len(const pg_wchar *from, unsigned char *to, int len)

## Detailed Description
This function performs a trivial conversion from PostgreSQL's internal wide character format (pg_wchar) to single-byte encoding. The conversion is accomplished by simply ignoring the high bits of each wide character and keeping only the low byte. This approach works for character sets where the wide character values fit within the single-byte range (0-255). The function processes characters until either the specified length is reached or a null wide character is encountered.

## Parameters / Member Variables
- `from`: Pointer to the source buffer containing wide characters (not necessarily null terminated)
- `to`: Pointer to the destination buffer for single-byte characters (caller must allocate sufficient space)
- `len`: Number of characters to process from the source buffer

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - direct character truncation)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (multiple references for various encoding configurations)

## Notes and Other Information
- Returns the number of characters successfully converted
- Null terminates the output buffer
- The conversion ignores high bits, which may result in data loss for characters outside the single-byte range
- Caller is responsible for ensuring the destination buffer has adequate space
- Used in PostgreSQL's character encoding conversion system for encodings that can be represented in single bytes