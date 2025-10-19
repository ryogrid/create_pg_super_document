# IsPagerNeeded

## Location
[src/fe_utils/print.c:3403-3442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3403-L3442)

## Overview
Determines whether a pager is needed for output and configures paging based on table content size and output settings.

## Definition

```c
static void
IsPagerNeeded(const printTableContent *cont, int extra_lines, bool expanded,
			  FILE **fout, bool *is_pager)
```
## Detailed Description
This static function calculates whether the output will require a pager based on the table content dimensions, format (expanded vs normal), and additional lines. It counts the total number of lines that will be displayed, including header, data rows, and footers, then calls PageOutput() to determine if paging is necessary based on terminal size and user preferences.

The function handles two output modes: expanded format (where each row spans multiple lines) and normal tabular format. It also accounts for footers when not in tuples-only mode, though the current implementation has a known limitation where it counts footer entries rather than actual footer lines.

## Parameters / Member Variables
- `*cont`: Pointer to the printTableContent structure containing the table data
- `extra_lines`: Additional lines to account for in the calculation (e.g., headers, spacing)
- `expanded`: Boolean indicating whether the table is in expanded format
- `**fout`: Pointer to FILE pointer that will be set to the appropriate output stream
- `*is_pager`: Pointer to boolean that will be set to indicate if pager is being used
## Dependencies
- Functions called/Symbols referenced:
  - [PageOutput](../P/PageOutput.md) (determines actual pager setup based on line count)
  - [printTableFooter](../p/printTableFooter.md) (structure type for footer counting)
- Called from (representative examples):
  - [print_aligned_text](../p/print_aligned_text.md) (print.c)
  - [print_aligned_vertical](../p/print_aligned_vertical.md) (print.c)
  - [printTable](../p/printTable.md) (print.c)

## Notes and Other Information
- Only activates paging when output is directed to stdout
- Uses different line counting logic for expanded vs normal table formats
- Contains a known FIXME: footer counting counts footer entries, not actual lines in footers
- Sets both the output file handle and pager status through pointer parameters
- Integrates with PostgreSQL's PageOutput system for terminal-aware paging
- Static function, only accessible within the print.c compilation unit

## Simplified Source

```c
static void IsPagerNeeded(const printTableContent *cont, int extra_lines,
                         bool expanded, FILE **fout, bool *is_pager) {
    if (*fout == stdout) {
        // Calculate total lines needed
        int lines;
        if (expanded)
            lines = (cont->ncolumns + 1) * cont->nrows;  // Each row spans multiple lines
        else
            lines = cont->nrows + 1;  // Normal table format + header

        // Add footer lines (if not tuples-only mode)
        if (!cont->opt->tuples_only) {
            // FIXME: counts footer entries, not actual lines
            for (printTableFooter *f = cont->footers; f; f = f->next)
                lines++;
        }

        // Setup pager based on total line count
        *fout = PageOutput(lines + extra_lines, cont->opt);
        *is_pager = (*fout != stdout);
    } else {
        *is_pager = false;
    }
}
```