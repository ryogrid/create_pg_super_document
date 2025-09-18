# pg_wcsformat

## Location
src/fe_utils/mbprint.c: 294 - 391

## Overview
Formats a multibyte character string into one or more lineptr structures for display purposes, handling special characters and multibyte sequences according to the specified encoding.

## Definition


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
- : Input multibyte character string to format
- : Length of the input string in bytes
- : Character encoding identifier for proper multibyte handling
- : Array of lineptr structures to store formatted output lines
- : Maximum number of lines available in the lines array

## Dependencies
- Functions called/Symbols referenced:
  - lineptr: Structure type for storing formatted line data
  - PQmblen: Determines byte length of multibyte characters
  - PQdsplen: Determines display width of multibyte characters
  - PG_UTF8: Encoding constant for UTF-8
  - utf8_to_unicode: Converts UTF-8 sequences to Unicode codepoints
- Called from (representative examples):
  - print_aligned_text: For formatting table cell contents
  - print_aligned_vertical: For vertical table formatting
  - lineptr: Through header inclusion for line formatting operations

## Notes and Other Information
- This function MUST be kept in sync with pg_wcssize for consistent behavior
- The function calls exit(1) if the lines array is insufficient, indicating a programming error
- Tab expansion follows standard 8-character tab stops
- Unicode escape sequences are only generated for UTF-8 encoding
- Each line in the output is null-terminated
- The lineptr array is terminated with a NULL ptr field in the final element
- Control character handling ensures safe display of potentially problematic input