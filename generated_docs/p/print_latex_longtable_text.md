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
- `*cont`: Pointer to printTableContent structure containing table data, headers, column alignments, border options, table attributes for column widths, and other formatting metadata
- `*fout`: File stream where the generated LaTeX longtable code will be written
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

## Simplified Source
```c
static void print_latex_longtable_text(const printTableContent *cont, FILE *fout)
{
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    unsigned int i;
    const char *opt_table_attr = cont->opt->tableAttr;
    const char *const *ptr;

    if (cancel_pressed)
        return;

    if (opt_border > 3)
        opt_border = 3;  // Clamp border level

    if (cont->opt->start_table) {
        // Begin longtable environment with column specifications
        fputs("\\begin{longtable}{", fout);

        if (opt_border >= 2) fputs("| ", fout);

        // Process column alignments and widths
        for (i = 0; i < cont->ncolumns; i++) {
            // For left-aligned columns with proportional width
            if (*(cont->aligns + i) == 'l' && opt_table_attr) {
                // Extract width from tableAttr and use p{width\textwidth}
                // [Complex width parsing logic omitted for simplicity]
                fputs("p{0.1\\textwidth}", fout);  // Default proportional width
            } else {
                fputc(*(cont->aligns + i), fout);  // Standard alignment
            }

            if (opt_border != 0 && i < cont->ncolumns - 1)
                fputs(" | ", fout);
        }

        if (opt_border >= 2) fputs(" |", fout);
        fputs("}\n", fout);

        // Set up longtable headers and footers
        if (!opt_tuples_only) {
            // First page header
            for (i = 0, ptr = cont->headers; i < cont->ncolumns; i++, ptr++) {
                if (i != 0) fputs(" & ", fout);
                fputs("\\small\\textbf{\\textit{", fout);
                latex_escaped_print(*ptr, fout);
                fputs("}}", fout);
            }
            fputs(" \\\\\n\\endfirsthead\n", fout);

            // Continuation headers
            fputs("\\caption{(Continued)} \\\\\n", fout);
            for (i = 0, ptr = cont->headers; i < cont->ncolumns; i++, ptr++) {
                if (i != 0) fputs(" & ", fout);
                fputs("\\small\\textbf{\\textit{", fout);
                latex_escaped_print(*ptr, fout);
                fputs("}}", fout);
            }
            fputs(" \\\\\n\\endhead\n", fout);
        }
    }

    // Print data cells with \raggedright for proportional columns
    for (i = 0, ptr = cont->cells; *ptr; i++, ptr++) {
        if (*(cont->aligns + i % cont->ncolumns) == 'l' && opt_table_attr)
            fputs("\\raggedright{", fout);

        latex_escaped_print(*ptr, fout);

        if (*(cont->aligns + i % cont->ncolumns) == 'l' && opt_table_attr)
            fputs("}", fout);

        if ((i + 1) % cont->ncolumns == 0) {
            fputs(" \\tabularnewline\n", fout);
            if (cancel_pressed) break;
        } else {
            fputs(" & ", fout);
        }
    }

    if (cont->opt->stop_table) {
        fputs("\\end{longtable}\n", fout);
    }
}
```