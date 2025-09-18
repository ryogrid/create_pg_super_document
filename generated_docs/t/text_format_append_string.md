# text_format_append_string

## Location
src/backend/utils/adt/varlena.c: 6090 - 6141

## Overview
Appends a string to a StringInfo buffer with optional padding and alignment based on specified flags and field width in PostgreSQL's text formatting system.

## Definition


## Detailed Description
This function handles string appending with formatting capabilities including field width control and alignment. It supports both left and right justification through flags or negative width values. When a field width is specified, the function pads the string with spaces to meet the required width. The function uses multibyte-aware string length calculation to properly handle Unicode characters.

## Parameters / Member Variables
- : StringInfo buffer where the formatted string will be appended
- : The input string to be formatted and appended
- : Formatting flags (TEXT_FORMAT_FLAG_MINUS for left alignment)
- : Field width for formatting (negative values imply left alignment)

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString
  - appendStringInfoSpaces
  - pg_mbstrlen
  - TEXT_FORMAT_FLAG_MINUS (constant)
- Called from:
  - text_format_string_conversion (multiple calls at lines 6052, 6054, 6069, 6075, 6080)

## Notes and Other Information
- Fast path optimization when width is 0 - simply appends the string without formatting
- Negative width values automatically enable left alignment and are converted to absolute values
- Uses pg_mbstrlen for multibyte character support to calculate proper string length
- Left justification: string first, then padding spaces
- Right justification: padding spaces first, then string
- Includes safety check for INT_MIN overflow when converting negative width to positive
- This function is a key component of PostgreSQL's format() function implementation