# print_aligned_vertical

## Location
src/fe_utils/print.c: 1324 - 1839

## Overview
Prints tabular data in vertical format where each record is displayed with column names on the left and values on the right, with support for wrapping, borders, and multiline content.

## Definition
```c
static void print_aligned_vertical(const printTableContent *cont, FILE *fout, bool is_pager)
```

## Detailed Description
This function renders tabular data in a vertical layout format, commonly used in PostgreSQL when displaying results with `\x` (expanded display) mode. Each row of the table is presented as a set of key-value pairs, with column headers displayed vertically alongside their corresponding data values. The function handles complex formatting scenarios including:

- Multiple border styles (0, 1, 2) for different visual presentations
- Automatic text wrapping when content exceeds available width
- Multi-line content handling within cells
- Record numbering and header formatting
- Interactive pager integration for large result sets
- Proper spacing and alignment calculations based on terminal width

The function performs extensive width calculations to determine optimal formatting, considering header widths, data widths, border requirements, and available terminal space. It supports both wrapped and unwrapped modes, automatically adjusting layout based on content size and terminal constraints.

## Parameters / Member Variables
- `cont`: Pointer to printTableContent structure containing the table data, headers, formatting options, and display preferences
- `fout`: File stream for output (typically stdout or a pager)
- `is_pager`: Boolean indicating whether output is being sent to a pager program

## Dependencies
- Functions called/Symbols referenced:
  - get_line_style
  - footers_with_default
  - IsPagerNeeded
  - pg_wcssize
  - pg_wcsformat
  - print_aligned_vertical_line
  - strlen_max_width
  - ClosePager
  - pg_malloc
- Called from (representative examples):
  - print_aligned_text
  - printTable

## Notes and Other Information
This function is part of PostgreSQL's frontend utility library and is primarily used by psql for displaying query results in expanded format. The function handles complex edge cases like mixed multiline headers and data, terminal width detection via ioctl, and proper memory management for formatting structures. It respects various display options including tuples_only mode, column width limits, and environmental variables like COLUMNS for width detection.