# CopyReadAttributesCSV

## Location
[src/backend/commands/copyfromparse.c:1791-1985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L1791-L1985)

## Overview
Parses a single line of CSV-format COPY data into separate attribute fields, handling CSV-specific features like quoted fields, escape sequences, and delimiter handling according to standard CSV conventions.

## Definition

```c
static int
CopyReadAttributesCSV(CopyFromState cstate)
```
## Detailed Description
This function serves as the CSV counterpart to , implementing RFC 4180-compliant CSV parsing for PostgreSQL's COPY operations. The parser handles the complexities of CSV format including quoted fields that can contain delimiters and newlines, escape sequences within quoted contexts, and proper handling of quote characters themselves.

The function implements a state machine approach with two primary modes:
1. **"Not in quote" mode**: Normal field parsing where delimiters separate fields
2. **"In quote" mode**: Quoted field parsing where content is preserved literally except for escape sequences

Key CSV-specific features include:
- **Quoted fields**: Fields enclosed in quote characters (typically double quotes) that can contain delimiters, newlines, and other special characters
- **Escape handling**: Within quoted fields, escape characters can be used to include literal quote or escape characters
- **Flexible delimiters**: Configurable field delimiter, quote character, and escape character
- **Null/default markers**: Support for NULL and DEFAULT value markers (only in unquoted fields)

The parser ensures strict CSV compliance by requiring proper termination of quoted fields and handling edge cases like empty fields and fields containing only whitespace.

## Parameters / Member Variables
- : The COPY operation state containing:
  - : Input line buffer with the raw CSV line to parse
  - : Output buffer for storing parsed field values
  - : Array of pointers to parsed field strings (NULL for null values)
  - : Current capacity of the raw_fields array
  - : The field delimiter character (typically comma)
  - : The field quote character (typically double quote)
  - : The escape character for quoted contexts
  - : String representation of NULL values
  - : String representation of DEFAULT markers
  - : Boolean array indicating which fields should use defaults
  - : Array of default expressions for each column

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes the attribute buffer
  - : Expands buffer capacity as needed
  - : Reallocates the raw_fields array for more columns
  - : Retrieves attribute numbers from the column list
  - : Reports CSV format errors with context
- Called from (representative examples):
  - : Main COPY parsing coordinator function

## Notes and Other Information
- Unlike text format, CSV format does not support backslash escape sequences (\n, \t, etc.) outside of the quote/escape mechanism
- Null and default markers are only recognized in unquoted fields to prevent ambiguity with legitimate quoted content
- The parser uses  statements for efficient state transitions and error handling in the field parsing loop
- Unterminated quoted fields generate specific error messages to help users identify CSV format issues  
- The function maintains the same API as  to allow transparent format switching
- Memory management follows the same optimization strategy as the text parser, pre-allocating buffers to avoid mid-parse reallocations
- The state machine design ensures proper handling of edge cases like adjacent quotes and escape sequences at field boundaries