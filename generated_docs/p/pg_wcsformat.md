# pg_wcsformat

## Location
[src/fe_utils/mbprint.c:294-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/mbprint.c#L294-L391)

## Overview
Formats a multibyte character string into one or more lineptr structures for display purposes, handling special characters and multibyte sequences according to the specified encoding.

## Definition

```c
void
pg_wcsformat(const unsigned char *pwcs, size_t len, int encoding,
			 struct lineptr *lines, int count)
```
## Detailed Description
pg_wcsformat takes a multibyte character string and formats it into an array of lineptr structures, where each structure represents a line of formatted output. This function is the companion to pg_wcssize and must be kept in sync with it. The function processes characters similarly to pg_wcssize but actually writes the formatted output to the provided lineptr array.

The function handles special formatting:
- Newlines (): Terminate current line and start a new one
- Carriage returns (): Convert to literal "\\r" string  
- Tabs (): Expand to spaces up to next 8-character boundary
- ASCII control characters: Format as hexadecimal escape sequences (\\xHH)
- Non-ASCII control characters: Format as Unicode escape sequences (\\uHHHH for UTF-8)
- Regular characters: Copy as-is for single-byte or copy all bytes for multibyte

## Parameters / Member Variables
- `*pwcs`: Input multibyte character string to format
- `len`: Length of the input string in bytes
- `encoding`: Character encoding identifier for proper multibyte handling
- `*lines`: Array of lineptr structures to store formatted output lines
- `count`: Maximum number of lines available in the lines array
## Dependencies
- Functions called/Symbols referenced:
  - [lineptr](../l/lineptr.md): Structure type for storing formatted line data
  - [PQmblen](../P/PQmblen.md): Determines byte length of multibyte characters
  - [PQdsplen](../P/PQdsplen.md): Determines display width of multibyte characters
  - PG_UTF8: Encoding constant for UTF-8
  - [utf8_to_unicode](../u/utf8_to_unicode.md): Converts UTF-8 sequences to Unicode codepoints
- Called from (representative examples):
  - [print_aligned_text](print_aligned_text.md): For formatting table cell contents
  - [print_aligned_vertical](print_aligned_vertical.md): For vertical table formatting
  - [lineptr](../l/lineptr.md): Through header inclusion for line formatting operations

## Notes and Other Information
- This function MUST be kept in sync with pg_wcssize for consistent behavior
- The function calls exit(1) if the lines array is insufficient, indicating a programming error
- Tab expansion follows standard 8-character tab stops
- Unicode escape sequences are only generated for UTF-8 encoding
- Each line in the output is null-terminated
- The lineptr array is terminated with a NULL ptr field in the final element
- Control character handling ensures safe display of potentially problematic input