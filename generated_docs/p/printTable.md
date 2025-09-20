# printTable

## Location
[src/fe_utils/print.c:3443-3548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3443-L3548)

## Overview
The printTable function is the main entry point for printing tabular data in various supported output formats within PostgreSQL client utilities, handling format selection, pager management, and output redirection.

## Definition

```c
void
printTable(const printTableContent *cont,
		   FILE *fout, bool is_pager, FILE *flog)
```
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
  - [IsPagerNeeded](../I/IsPagerNeeded.md) (for automatic pager decision)
  - clearerr (to clear output stream errors)
  - [print_aligned_text](print_aligned_text.md) (for aligned text output)
  - [print_unaligned_vertical](print_unaligned_vertical.md)/print_unaligned_text (for unaligned output)
  - [print_aligned_vertical](print_aligned_vertical.md)/print_aligned_text (for aligned/wrapped output)
  - [print_csv_vertical](print_csv_vertical.md)/print_csv_text (for CSV output)
  - [print_html_vertical](print_html_vertical.md)/print_html_text (for HTML output)
  - [print_asciidoc_vertical](print_asciidoc_vertical.md)/print_asciidoc_text (for AsciiDoc output)
  - [print_latex_vertical](print_latex_vertical.md)/print_latex_text (for LaTeX output)
  - [print_latex_longtable_text](print_latex_longtable_text.md) (for LaTeX longtable output)
  - [print_troff_ms_vertical](print_troff_ms_vertical.md)/print_troff_ms_text (for troff-ms output)
  - [ClosePager](../C/ClosePager.md) (to close locally opened pager)
  - PRINT_* format constants
- Called from (representative examples):
  - [printQuery](printQuery.md) (main query result printing)
  - [printCrosstab](printCrosstab.md) (crosstab view output)
  - [describeOneTableDetails](../d/describeOneTableDetails.md) (table description output)
  - [describeRoles](../d/describeRoles.md) (role description output)

## Notes and Other Information
- The function handles cancellation gracefully by checking cancel_pressed at the start
- Pager management is format-dependent: aligned and wrapped formats handle paging internally, while other formats rely on this function for pager setup
- When both fout and flog are specified, the log output always uses aligned text format regardless of the primary output format
- The expanded mode (vertical display) is supported across all output formats for better readability of wide tables
- Error handling includes clearing pre-existing errors on the output stream and reporting invalid format errors to stderr
- The function exits with EXIT_FAILURE for internal errors related to invalid output formats