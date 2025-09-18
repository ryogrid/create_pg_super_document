# IsPagerNeeded

## Location
src/fe_utils/print.c: 3403 - 3442

## Overview
Determines whether a pager is needed for output and configures paging based on table content size and output settings.

## Definition


## Detailed Description
This static function calculates whether the output will require a pager based on the table content dimensions, format (expanded vs normal), and additional lines. It counts the total number of lines that will be displayed, including header, data rows, and footers, then calls PageOutput() to determine if paging is necessary based on terminal size and user preferences.

The function handles two output modes: expanded format (where each row spans multiple lines) and normal tabular format. It also accounts for footers when not in tuples-only mode, though the current implementation has a known limitation where it counts footer entries rather than actual footer lines.

## Parameters / Member Variables
- : Pointer to the printTableContent structure containing the table data
- : Additional lines to account for in the calculation (e.g., headers, spacing)
- : Boolean indicating whether the table is in expanded format
- : Pointer to FILE pointer that will be set to the appropriate output stream
- : Pointer to boolean that will be set to indicate if pager is being used

## Dependencies
- Functions called/Symbols referenced:
  - PageOutput (determines actual pager setup based on line count)
  - printTableFooter (structure type for footer counting)
- Called from (representative examples):
  - print_aligned_text (print.c)
  - print_aligned_vertical (print.c)
  - printTable (print.c)

## Notes and Other Information
- Only activates paging when output is directed to stdout
- Uses different line counting logic for expanded vs normal table formats
- Contains a known FIXME: footer counting counts footer entries, not actual lines in footers
- Sets both the output file handle and pager status through pointer parameters
- Integrates with PostgreSQL's PageOutput system for terminal-aware paging
- Static function, only accessible within the print.c compilation unit