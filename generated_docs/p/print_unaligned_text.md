# print_unaligned_text

## Location
src/fe_utils/print.c: 422 - 512

## Overview
Prints table data in unaligned text format where fields are separated by configurable delimiters without column alignment or borders.

## Definition


## Detailed Description
This function renders tabular data in a simple unaligned text format, primarily used for machine-readable output or when visual alignment is not required. The function processes the table content sequentially, printing the title, headers, data cells, and footers separated by configurable field and record separators. It supports both human-readable and machine-readable output modes through the tuples_only option, and handles special cases like zero-byte record separators for compatibility with Unix tools like find -print0 and xargs.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing the table data, headers, options, and formatting settings
- : FILE pointer to the output stream where the formatted text will be written

## Dependencies
- Functions called/Symbols referenced:
  - print_separator (for field and record separation)
  - footers_with_default (to get default footers if needed)
  - fputs (standard C library function for string output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - printTable (main table printing dispatcher function)

## Notes and Other Information
- Uses configurable field separators (fieldSep) between columns and record separators (recordSep) between rows
- Respects the tuples_only option to suppress headers, title, and footers for machine-readable output
- Handles cancellation through the global cancel_pressed variable for responsive user interaction
- Special handling for zero-byte record separators to maintain compatibility with Unix pipeline tools
- The last record is always terminated with a newline unless using zero-byte record separator mode
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem