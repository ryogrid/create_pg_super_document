# print_latex_text

## Location
[src/fe_utils/print.c:2454-2560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2454-L2560)

## Overview
Formats and prints table data in standard LaTeX tabular environment format with support for borders, alignment, titles, headers, and footers.

## Definition

```c
static void
print_latex_text(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function generates LaTeX code for displaying tabular data using the standard tabular environment. It creates a complete LaTeX table with configurable borders (0-3 levels), proper column alignment specifications, optional titles centered above the table, headers in italics, and footers below the table. The function handles LaTeX-specific formatting requirements including proper escaping of special characters and appropriate use of LaTeX table syntax including column separators (&) and row terminators (\\).

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing table data, headers, alignment specifications, border options, and other formatting metadata
- `*fout`: File stream where the generated LaTeX tabular code will be written
## Dependencies
- Functions called/Symbols referenced:
  - [latex_escaped_print](../l/latex_escaped_print.md) (for escaping special LaTeX characters in content)
  - [footers_with_default](../f/footers_with_default.md) (to get footer information with defaults applied)
  - [printTableContent](printTableContent.md) (data structure)
  - [printTableFooter](printTableFooter.md) (data structure)
  - cancel_pressed (global variable for interrupt handling)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- This is a static function within print.c used internally for LaTeX table formatting
- Supports 4 border levels: 0 (no borders), 1 (column separators only), 2 (full borders), 3 (borders between all rows)
- Border level is clamped to maximum of 3 if higher values are provided
- Title is rendered in a centered environment above the table
- Headers are formatted in italics using \textit{} command
- Column alignments are taken directly from cont->aligns (typically 'l', 'c', 'r' for left, center, right)
- Uses \hline for horizontal rules and | for vertical borders in column specification
- Handles cancellation via cancel_pressed global variable for responsive interruption
- Footers are printed with line breaks and no indentation after the table ends

## Simplified Source
```c
static void print_latex_text(const printTableContent *cont, FILE *fout)
{
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    unsigned int i;
    const char *const *ptr;

    if (cancel_pressed)
        return;

    if (opt_border > 3)
        opt_border = 3;  // Clamp border level

    if (cont->opt->start_table) {
        // Print centered title
        if (!opt_tuples_only && cont->title) {
            fputs("\\begin{center}\n", fout);
            latex_escaped_print(cont->title, fout);
            fputs("\n\\end{center}\n\n", fout);
        }

        // Begin tabular environment with column specs
        fputs("\\begin{tabular}{", fout);

        if (opt_border >= 2) fputs("| ", fout);  // Left border

        for (i = 0; i < cont->ncolumns; i++) {
            fputc(*(cont->aligns + i), fout);     // Column alignment
            if (opt_border != 0 && i < cont->ncolumns - 1)
                fputs(" | ", fout);              // Column separators
        }

        if (opt_border >= 2) fputs(" |", fout);  // Right border
        fputs("}\n", fout);

        if (!opt_tuples_only && opt_border >= 2)
            fputs("\\hline\n", fout);            // Top border

        // Print headers in italics
        if (!opt_tuples_only) {
            for (i = 0, ptr = cont->headers; i < cont->ncolumns; i++, ptr++) {
                if (i != 0) fputs(" & ", fout);
                fputs("\\textit{", fout);
                latex_escaped_print(*ptr, fout);
                fputc('}', fout);
            }
            fputs(" \\\\\n\\hline\n", fout);
        }
    }

    // Print data cells
    for (i = 0, ptr = cont->cells; *ptr; i++, ptr++) {
        latex_escaped_print(*ptr, fout);

        if ((i + 1) % cont->ncolumns == 0) {
            fputs(" \\\\\n", fout);               // End row
            if (opt_border == 3)
                fputs("\\hline\n", fout);         // Row separators
            if (cancel_pressed) break;
        } else {
            fputs(" & ", fout);                   // Column separator
        }
    }

    if (cont->opt->stop_table) {
        if (opt_border == 2)
            fputs("\\hline\n", fout);             // Bottom border

        fputs("\\end{tabular}\n\n\\noindent ", fout);

        // Print footers
        printTableFooter *footers = footers_with_default(cont);
        if (footers && !opt_tuples_only && !cancel_pressed) {
            for (printTableFooter *f = footers; f; f = f->next) {
                latex_escaped_print(f->data, fout);
                fputs(" \\\\\n", fout);
            }
        }
        fputc('\n', fout);
    }
}
```