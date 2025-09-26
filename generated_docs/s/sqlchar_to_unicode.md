# sqlchar_to_unicode

## Location
[src/backend/utils/adt/xml.c:2336-2354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2336-L2354)

## Overview
A static function that converts a single character from the current server encoding to its corresponding Unicode codepoint, handling multi-byte character encodings through UTF-8 conversion.

## Definition

```c
static pg_wchar
sqlchar_to_unicode(const char *s)
```
## Detailed Description
This function performs character encoding conversion by taking a character in the server's current encoding and converting it to a Unicode codepoint (pg_wchar). The conversion process involves two main steps: first converting the character from the server encoding to UTF-8 using pg_server_to_any(), then converting the UTF-8 representation to a wide character (Unicode codepoint) using pg_encoding_mb2wchar_with_len().

The function is designed to handle multi-byte characters correctly by determining the character length in the source encoding using pg_mblen() and the UTF-8 length using pg_encoding_mblen(). Memory management is handled carefully - if a conversion to UTF-8 was necessary (i.e., the server encoding is not UTF-8), the allocated UTF-8 string is freed after use.

## Parameters / Member Variables
- : A pointer to the character string in the server's current encoding. Note that this is not assumed to be null-terminated, so the function relies on encoding-specific length functions to determine character boundaries.

## Dependencies
- Functions called/Symbols referenced:
  - pg_server_to_any (converts from server encoding to UTF-8)
  - pg_mblen (determines character length in server encoding)
  - pg_encoding_mb2wchar_with_len (converts UTF-8 to Unicode codepoint)
  - pg_encoding_mblen (determines character length in UTF-8)
  - pfree (frees allocated memory when needed)
  - PG_UTF8 (UTF-8 encoding constant)
- Called from (representative examples):
  - map_sql_identifier_to_xml_name

## Notes and Other Information
- This is a static function, accessible only within the xml.c compilation unit
- The function handles memory management automatically, freeing the intermediate UTF-8 string if conversion was necessary
- The ret array is sized for 2 elements to accommodate the trailing zero required by pg_encoding_mb2wchar_with_len
- The function is primarily used in XML processing contexts where SQL identifiers need to be converted to valid XML names
- Multi-byte character support is essential for international character sets and proper XML name generation
- The function does not assume null-terminated input, making it safe for use with substring operations