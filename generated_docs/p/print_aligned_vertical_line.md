# print_aligned_vertical_line

## Location
src/fe_utils/print.c: 1225 - 1323

## Overview
Draws horizontal separating lines for aligned vertical table format, including record separators with optional record numbers and proper border formatting.

## Definition


## Detailed Description
This utility function generates horizontal separator lines specifically for vertical (record-oriented) table layouts. It creates lines that separate individual records and includes optional record numbering ("* Record N" or "[ RECORD N ]"). The function handles different border styles and can dynamically adjust line width based on terminal width constraints and expanded header width settings. It supports various header width modes including page-width, exact-width, and column-width formatting.

## Parameters / Member Variables
- : Pointer to printTableOpt structure containing table formatting options and settings
- : Record number to display in the separator (0 means no record number)
- : Width allocated for the header portion of the line
- : Width allocated for the data portion of the line  
- : Available terminal width for output formatting
- : Position type of the line (top, middle, bottom) as defined by printTextRule enum
- : FILE pointer to the output stream where the line will be written

## Dependencies
- Functions called/Symbols referenced:
  - [get_line_style](../g/get_line_style.md) (gets formatting characters for the current line style)
  - [printTextLineFormat](printTextLineFormat.md) (structure for line formatting rules)
  - [printTextRule](printTextRule.md) (enum for line position types)
  - [printTableOpt](printTableOpt.md) (structure containing table options)
  - PRINT_XHEADER_COLUMN, PRINT_XHEADER_PAGE, PRINT_XHEADER_EXACT_WIDTH (constants for header width modes)
  - fprintf (standard C library function for formatted output)
  - fputs (standard C library function for string output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - [print_aligned_vertical](print_aligned_vertical.md) (for drawing record separators in vertical table format)

## Notes and Other Information
- Specialized function for vertical table layout, complementing _print_horizontal_line for horizontal layouts
- Handles record numbering with different formats based on border style ("* Record N" vs "[ RECORD N ]")
- Supports dynamic width adjustment based on terminal width and header width type settings
- Uses different border characters (leftvrule, rightvrule, midvrule, hrule) based on formatting rules
- Implements intelligent width calculations that respect terminal boundaries while maintaining proper alignment
- Handles three header width modes: column-based, page-based, and exact width specification  
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem
- Essential for creating properly formatted record separators in PostgreSQL's expanded (vertical) display mode
- Provides visual separation between records when displaying wide tables in vertical format