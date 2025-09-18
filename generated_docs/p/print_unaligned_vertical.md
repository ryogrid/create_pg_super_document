# print_unaligned_vertical

## Location
src/fe_utils/print.c: 513 - 592

## Overview
Prints table data in unaligned vertical format where each field appears on its own line with the column header, creating a record-oriented output.

## Definition


## Detailed Description
This function renders tabular data in a vertical (record-oriented) format where each row is displayed as a series of "header: value" pairs, with each field on its own line. This format is particularly useful for displaying wide tables or when you want to see each record's complete information in a readable vertical layout. The function uses double record separators to distinguish between different table records and includes support for titles and footers when not in tuples-only mode.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing the table data, headers, options, and formatting settings
- : FILE pointer to the output stream where the formatted text will be written

## Dependencies
- Functions called/Symbols referenced:
  - [print_separator](print_separator.md) (for field and record separation)
  - fputs (standard C library function for string output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- Uses vertical format where each field appears as "header<fieldSep>value" on its own line
- Record separators appear twice between records to visually separate different rows
- Within each record, individual field lines are separated by single record separators
- Respects the tuples_only option to suppress title and footers for machine-readable output
- Handles cancellation through the global cancel_pressed variable for responsive user interaction
- Special handling for zero-byte record separators to maintain compatibility with Unix pipeline tools
- The last record is terminated with a newline unless using zero-byte record separator mode
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem
- Particularly useful for displaying results with many columns or when each record needs detailed inspection