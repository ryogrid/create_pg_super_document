# printTable

## Location
src/fe_utils/print.c: 3443 - 3548

## Overview
The printTable function is the main entry point for printing tabular data in various supported output formats within PostgreSQL client utilities, handling format selection, pager management, and output redirection.

## Definition


## Detailed Description
This function serves as the central dispatcher for table printing operations in PostgreSQL client tools like psql. It takes table content with formatting options and renders it according to the specified output format. The function supports multiple output formats including aligned text, unaligned text, CSV, HTML, AsciiDoc, LaTeX, and troff-ms. It intelligently manages pager usage for large outputs and can simultaneously write to both the main output and a log file when specified.

The function first checks for cancellation and whether output should be suppressed (PRINT_NOTHING format). It then determines whether to use a pager for formats that don't handle paging internally. Based on the format specified in the content options, it dispatches to the appropriate format-specific printing function, with special handling for expanded (vertical) vs. normal (horizontal) display modes.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing the table data, column headers, formatting options, and display preferences
- : File pointer for the primary output destination (stdout, file, or pager pipe)
- : Boolean indicating whether the caller has already set up fout as a pager pipe
- : Optional file pointer for simultaneous logging output (used with --log-file option)

## Dependencies
- Functions called/Symbols referenced:
  - IsPagerNeeded (for automatic pager decision)
  - clearerr (to clear output stream errors)
  - print_aligned_text (for aligned text output)
  - print_unaligned_vertical/print_unaligned_text (for unaligned output)
  - print_aligned_vertical/print_aligned_text (for aligned/wrapped output)
  - print_csv_vertical/print_csv_text (for CSV output)
  - print_html_vertical/print_html_text (for HTML output)
  - print_asciidoc_vertical/print_asciidoc_text (for AsciiDoc output)
  - print_latex_vertical/print_latex_text (for LaTeX output)
  - print_latex_longtable_text (for LaTeX longtable output)
  - print_troff_ms_vertical/print_troff_ms_text (for troff-ms output)
  - ClosePager (to close locally opened pager)
  - PRINT_* format constants
- Called from (representative examples):
  - printQuery (main query result printing)
  - printCrosstab (crosstab view output)
  - describeOneTableDetails (table description output)
  - describeRoles (role description output)

## Notes and Other Information
- The function handles cancellation gracefully by checking cancel_pressed at the start
- Pager management is format-dependent: aligned and wrapped formats handle paging internally, while other formats rely on this function for pager setup
- When both fout and flog are specified, the log output always uses aligned text format regardless of the primary output format
- The expanded mode (vertical display) is supported across all output formats for better readability of wide tables
- Error handling includes clearing pre-existing errors on the output stream and reporting invalid format errors to stderr
- The function exits with EXIT_FAILURE for internal errors related to invalid output formats