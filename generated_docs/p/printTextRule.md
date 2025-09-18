# printTextRule

## Location
src/include/fe_utils/print.h: 59 - 60

## Overview
printTextRule is an enumeration type that defines different types of horizontal rules used for drawing table borders and separators in PostgreSQL's frontend text output formatting.

## Definition

(Defined in src/include/fe_utils/print.h:52-59)

## Detailed Description
The printTextRule enumeration provides context for selecting appropriate line drawing characters when rendering table output in PostgreSQL's frontend utilities. It categorizes different types of horizontal rules that may appear in formatted table output, allowing the printing system to choose appropriate characters or formatting for each rule type. This is particularly important for creating visually appealing table borders and separators in terminal output.

## Parameters / Member Variables
- : Represents the top horizontal border line of a table
- : Represents horizontal separator lines between data rows within a table
- : Represents the bottom horizontal border line of a table
- : Represents data rows where horizontal rule characters are not used

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration type definition)
- Called from (representative examples):
  - _print_horizontal_line (src/fe_utils/print.c:594)
  - print_aligned_vertical_line (src/fe_utils/print.c:1230)
  - print_aligned_vertical (src/fe_utils/print.c:1589)

## Notes and Other Information
- This enumeration is used in conjunction with printTextLineFormat structures to determine the appropriate line drawing characters for different parts of table output
- The PRINT_RULE_DATA value indicates that horizontal rule characters are not used for regular data lines
- Essential for creating consistent and visually appealing table formatting in PostgreSQL's command-line tools, particularly psql
- Works with Unicode and ASCII line drawing character sets depending on terminal capabilities