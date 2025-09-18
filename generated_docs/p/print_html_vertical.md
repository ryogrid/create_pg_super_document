# print_html_vertical

## Location
[src/fe_utils/print.c:2082-2167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2082-L2167)

## Overview
Renders table data in vertical HTML format where each row is displayed as a series of field-value pairs, useful for displaying detailed record information.

## Definition


## Detailed Description
This function generates HTML output in vertical format, displaying table data as field-value pairs rather than traditional tabular rows and columns. Each record is presented with its fields listed vertically, where column headers become row labels and the corresponding data values are displayed alongside them. This format is particularly useful for displaying detailed information about individual records or when dealing with tables that have many columns that would be difficult to read in traditional horizontal format. The function includes record numbering and handles all HTML escaping to prevent injection attacks.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing table data, headers, formatting options, and configuration
- : Output file stream where the vertical HTML table will be written

## Dependencies
- Functions called/Symbols referenced:
  - [html_escaped_print](../h/html_escaped_print.md) (for escaping HTML content)
  - fprintf, fputs, fputc (standard C library functions)
  - strspn (standard C library function)
- Called from:
  - [printTable](printTable.md) (src/fe_utils/print.c:3501)

## Notes and Other Information
- Displays data in vertical format with field names in header cells and values in data cells
- Each record is numbered and separated by a spanning row header (unless in tuples-only mode)
- Creates two-column layout where first column contains field names, second contains values
- Each field-value pair gets its own table row
- Record separators use colspan=2 to span the full table width
- Empty or whitespace-only values are rendered as '&nbsp;' to maintain visual structure
- Supports tuples-only mode which omits record numbers and uses empty separators
- All content is HTML-escaped to prevent markup injection
- Includes cancellation support during processing
- Uses same alignment settings as horizontal mode for data values
- Footers are rendered as paragraph elements when table processing completes