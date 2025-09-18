# printXheaderWidthType

## Location
src/include/fe_utils/print.h: 79 - 80

## Overview
printXheaderWidthType is an enumeration type that defines different width handling strategies for header lines in PostgreSQL's expanded/vertical output mode.

## Definition

(Defined in src/include/fe_utils/print.h:69-79)

## Detailed Description
The printXheaderWidthType enumeration controls how header lines are formatted and sized in PostgreSQL's expanded (vertical) output mode. In expanded mode, query results are displayed vertically rather than in traditional tabular format, and this enumeration provides different strategies for handling the width of separator lines and headers. This is particularly useful for controlling output formatting when dealing with wide data that might exceed terminal boundaries or when specific formatting constraints are desired.

## Parameters / Member Variables
- : Header lines are not truncated and can extend to their full natural width (default behavior)
- : Header line is only printed above the first column, providing minimal header formatting
- : Header line width is constrained to not exceed the terminal width, ensuring it fits within display boundaries
- : Header line uses an explicitly specified width value, providing precise control over formatting

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration type definition)
- Called from (representative examples):
  - printTableOpt (src/include/fe_utils/print.h:116) - used as member variable expanded_header_width_type

## Notes and Other Information
- This enumeration is specifically designed for PostgreSQL's expanded output mode, which displays query results in a vertical format rather than traditional table format
- Used in conjunction with the printTableOpt structure to configure table output formatting options
- The PRINT_XHEADER_EXACT_WIDTH option works with the expanded_header_exact_width integer field in printTableOpt to provide precise width control
- Essential for creating readable output in terminals with varying widths and for different user preferences regarding header line formatting
- Primarily used in psql's \x (expanded) display mode