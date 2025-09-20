# print_latex_longtable_text

## Location
[src/fe_utils/print.c:2561-2590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2561-L2590)

## Overview
Formats and prints table data using LaTeX's longtable environment, which supports multi-page tables with repeated headers and footers.

## Definition

```c
static void
print_latex_longtable_text(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function generates LaTeX code using the longtable package, which is designed for tables that can span multiple pages. It creates sophisticated table layouts with first headers, continuing headers, footers, and last footers. The function supports proportional column widths through the tableAttr option, where widths can be specified as fractions of \textwidth. It uses professional table formatting with \toprule, \midrule, and \bottomrule commands, and supports captions that appear on continuation pages with "(Continued)" notation.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing table data, headers, column alignments, border options, table attributes for column widths, and other formatting metadata
- : File stream where the generated LaTeX longtable code will be written

## Dependencies
- Functions called/Symbols referenced:
  - [latex_escaped_print](../l/latex_escaped_print.md) (for escaping special LaTeX characters in content)
  - [printTableContent](printTableContent.md) (data structure)
  - cancel_pressed (global variable for interrupt handling)
  - LONGTABLE_WHITESPACE (macro defining whitespace characters)
  - strspn, strcspn, fwrite (standard C library functions for string processing)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- This is a static function within print.c used internally for LaTeX longtable formatting
- Supports proportional column widths using p{width\textwidth} syntax for left-aligned columns
- Uses sophisticated longtable features: \endfirsthead, \endhead, \endfoot, \endlastfoot
- Headers are formatted with \small\textbf{\textit{}} for emphasis
- Table cells use \raggedright{} for consistent left alignment within proportional columns
- Supports 4 border levels like regular LaTeX tables but uses professional booktabs-style rules
- Handles table captions with continuation notation for multi-page tables
- Uses \tabularnewline instead of \\\\ for better longtable compatibility
- Border level is clamped to maximum of 3 if higher values are provided
- Processes tableAttr string to extract proportional widths, reusing previous values for subsequent columns