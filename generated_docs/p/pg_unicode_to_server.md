# pg_unicode_to_server

## Location
src/backend/utils/mb/mbutils.c: 864 - 925

## Overview
Converts a single Unicode code point into a string representation in the server's encoding, providing essential Unicode-to-database encoding conversion functionality.

## Definition
```c
void pg_unicode_to_server(pg_wchar c, unsigned char *s)
```

## Detailed Description
This function converts a Unicode code point to the server's database encoding with several optimization paths:

1. **Validation**: Checks if the Unicode code point is valid using `is_valid_unicode_codepoint()`
2. **ASCII optimization**: For code points ≤ 0x7F, directly copies as single bytes
3. **UTF-8 server encoding**: When database uses UTF-8, converts directly using `unicode_to_utf8()`
4. **General conversion**: For other encodings, converts via UTF-8 intermediate representation using cached conversion procedures

The function is designed to work outside transactions and in aborted transactions, relying on pre-cached conversion functions for reliability. Output is null-terminated and fits within MAX_UNICODE_EQUIVALENT_STRING+1 bytes.

## Parameters / Member Variables
- `c`: Unicode code point (pg_wchar) to convert
- `s`: Output buffer with at least MAX_UNICODE_EQUIVALENT_STRING+1 bytes available, receives null-terminated converted string

## Dependencies
- Functions called/Symbols referenced:
  - is_valid_unicode_codepoint (Unicode validation)
  - GetDatabaseEncoding (database encoding detection)
  - unicode_to_utf8 (Unicode to UTF-8 conversion)
  - pg_utf_mblen (UTF-8 character length calculation)
  - GetDatabaseEncodingName (encoding name lookup)
  - FunctionCall6 (PostgreSQL function call interface)
- Called from (representative examples):
  - str_udeescape (string escape sequence processing)
  - unistr (SQL UNISTR function implementation)
  - map_xml_name_to_sql_identifier (XML identifier conversion)

## Notes and Other Information
- Requires pre-cached conversion functions (Utf8ToServerConvProc) to be available
- Safe to call outside transactions or in aborted transactions due to cached procedure design
- Throws errors for invalid Unicode code points or unsupported encoding conversions
- Uses UTF-8 as intermediate representation for non-UTF-8 database encodings
- Part of PostgreSQL's comprehensive Unicode support infrastructure
- Located in src/backend/utils/mb/mbutils.c:864-925