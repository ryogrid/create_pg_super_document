# CopyReadAttributesText

## Location
src/backend/commands/copyfromparse.c: 1537 - 1790

## Overview
Parses a single line of text-format COPY data into separate attribute fields, performing character de-escaping and handling null/default markers according to PostgreSQL's text format specifications.

## Definition


## Detailed Description
This function is the core parser for text-format COPY operations in PostgreSQL. It processes the current input line stored in , separating it into individual field values based on the configured delimiter character. The function performs sophisticated escape sequence processing, converting backslash-escaped characters (like , , octal sequences , and hexadecimal sequences ) back to their literal values.

The function handles three types of field values:
1. **Regular data**: Standard field content with escape processing
2. **NULL markers**: Fields matching the configured null representation string
3. **DEFAULT markers**: Fields matching the configured default value marker (when defaults are enabled)

Key features include:
- Efficient single-pass parsing with speculative de-escaping
- Dynamic memory management for variable field counts
- Character encoding validation for non-ASCII characters
- Support for zero-column tables
- Robust error handling with meaningful error messages

The parsed field values are stored as null-terminated strings in , with NULL pointers indicating null values.

## Parameters / Member Variables
- : The COPY operation state containing:
  - : Input line buffer containing the raw text to parse
  - : Output buffer for storing de-escaped field values
  - : Array of pointers to parsed field strings (NULL for null values)
  - : Current size of the raw_fields array (dynamically expanded)
  - : The field delimiter character
  - : String representation of NULL values
  - : String representation of DEFAULT markers
  - : Boolean array tracking which fields use default values
  - : Array of default value expressions for each column

## Dependencies
- Functions called/Symbols referenced:
  - : Clears the attribute buffer
  - : Expands buffer capacity
  - : Reallocates the raw_fields array
  - : Macros for octal digit processing
  - : Converts hex digits to decimal
  - : Checks for non-ASCII characters
  - : Retrieves attribute numbers from column list
  - : Validates multi-byte character encoding
- Called from (representative examples):
  - : Main COPY parsing coordinator function

## Notes and Other Information
- The function uses speculative de-escaping, performing escape processing while scanning but only committing the results after null/default marker checks
- Memory management is optimized to avoid frequent reallocations by pre-sizing buffers based on input length
- The parser supports PostgreSQL's full escape sequence syntax including octal (\001-\377) and hexadecimal (\x00-\xFF) representations
- Non-ASCII characters generated through escape sequences trigger encoding validation to ensure database compatibility
- Zero-column table handling is a special case that simply validates empty input lines
- Error reporting includes detailed context about column names and expected formats for better user experience