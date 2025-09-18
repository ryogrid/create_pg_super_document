# print_aligned_text

## Location
[src/fe_utils/print.c:635-1224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L635-L1224)

## Overview
Renders tabular data in aligned text format with proper column borders, spacing, and text wrapping support for creating professional-looking table output.

## Definition


## Detailed Description
This is the most sophisticated table formatting function in PostgreSQL's printing subsystem. It creates well-formatted tables with aligned columns, configurable borders (none, single, double), automatic text wrapping when content exceeds available width, and intelligent column width optimization. The function handles complex scenarios including multi-line cells, variable-width character encodings, automatic pager invocation for large output, and responsive formatting that adapts to terminal width. It supports both horizontal and vertical layout modes, with automatic switching to vertical mode when the table is too wide for the available display width.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing table data, headers, alignment settings, and formatting options
- : FILE pointer to the output stream where the formatted table will be written
- : Boolean indicating whether output is going to a pager (affects width calculations and formatting decisions)

## Dependencies
- Functions called/Symbols referenced:
  - [get_line_style](../g/get_line_style.md) (gets formatting characters for the current line style)
  - [_print_horizontal_line](_print_horizontal_line.md) (draws horizontal border lines)
  - [pg_wcssize](pg_wcssize.md) (calculates display width of wide character strings)
  - [pg_wcsformat](pg_wcsformat.md) (formats wide character strings for display)
  - strlen_max_width (calculates byte length up to a display width limit)
  - [print_aligned_vertical](print_aligned_vertical.md) (alternative vertical layout for wide tables)
  - [PageOutput](../P/PageOutput.md) (initiates pager for large output)
  - [IsPagerNeeded](../I/IsPagerNeeded.md) (determines if pager is required)
  - [footers_with_default](../f/footers_with_default.md) (gets table footers with defaults)
  - pg_malloc0/pg_malloc (memory allocation functions)
  - Various PRINT_RULE_* and PRINT_LINE_WRAP_* constants for formatting states
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- Most complex function in the PostgreSQL table formatting system with ~590 lines of code
- Implements intelligent column width optimization that shrinks columns with high max/average width ratios
- Supports automatic text wrapping with configurable wrap points and visual indicators
- Handles multi-byte character encodings correctly for international text display
- Automatically invokes pager when output exceeds terminal dimensions
- Can switch to vertical layout mode (print_aligned_vertical) when table is too wide
- Supports three border styles: 0=none, 1=simple, 2=full borders with corner characters
- Handles column alignment (left/right) specified in cont->aligns array
- Processes embedded newlines in cell data and creates multi-line table rows
- Uses sophisticated memory management with multiple dynamically allocated arrays
- Function is static, indicating it's only used within the print.c module
- Essential for creating the professional table output that PostgreSQL is known for in psql client