# print_latex_vertical

## Location
[src/fe_utils/print.c:2717-2809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2717-L2809)

## Overview
Prints table data in LaTeX vertical format where each record is displayed as field-value pairs in a two-column layout with headers and data side by side.

## Definition

```c
static void
print_latex_vertical(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function formats tabular data in a vertical LaTeX layout using a two-column table structure where the left column contains field names (headers) and the right column contains the corresponding values. Each record is preceded by a "Record N" header that spans both columns. The function uses a standard tabular environment with configurable borders and supports optional titles displayed in a centered environment above the table. Record numbering continues from previous tables using prior_records.

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing table data, headers, border options, titles, footers, and formatting metadata
- `*fout`: File stream where the generated vertical LaTeX table code will be written
## Dependencies
- Functions called/Symbols referenced:
  - [latex_escaped_print](../l/latex_escaped_print.md) (for escaping special LaTeX characters in headers and content)
  - [printTableContent](printTableContent.md) (data structure)
  - [printTableFooter](printTableFooter.md) (data structure)
  - cancel_pressed (global variable for interrupt handling)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function, called for both regular vertical and when converting from other formats)

## Notes and Other Information
- This is a static function within print.c used internally for vertical LaTeX table formatting
- Uses a two-column tabular layout: left column for field names, right column for values
- Supports 3 border levels: 0 (no borders), 1 (center divider), 2 (full borders around entire table)
- Border level is clamped to maximum of 2 (unlike other LaTeX functions that support level 3)
- Record headers use \multicolumn{2}{c}{} or \multicolumn{2}{|c|}{} to span both columns
- Record numbering starts from cont->opt->prior_records + 1 for pagination support
- Uses \textit{} for record headers and field names formatting
- Titles are centered above the table using \begin{center}...\end{center}
- Footers are printed with line breaks and no indentation after table completion
- Handles cancellation via cancel_pressed global variable for responsive interruption

## Simplified Source
```c
static void print_latex_vertical(const printTableContent *cont, FILE *fout)
{
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    unsigned long record = cont->opt->prior_records + 1;
    unsigned int i;
    const char *const *ptr;

    if (cancel_pressed)
        return;

    if (opt_border > 2)
        opt_border = 2;  // Clamp border level (max 2 for vertical)

    if (cont->opt->start_table) {
        // Print centered title
        if (!opt_tuples_only && cont->title) {
            fputs("\\begin{center}\n", fout);
            latex_escaped_print(cont->title, fout);
            fputs("\n\\end{center}\n\n", fout);
        }

        // Create two-column table with appropriate borders
        fputs("\\begin{tabular}{", fout);
        if (opt_border == 0)
            fputs("cl", fout);           // No borders
        else if (opt_border == 1)
            fputs("c|l", fout);          // Center divider only
        else
            fputs("|c|l|", fout);        // Full borders
        fputs("}\n", fout);
    }

    // Print records as field-value pairs
    for (i = 0, ptr = cont->cells; *ptr; i++, ptr++) {
        // Start new record
        if (i % cont->ncolumns == 0) {
            if (cancel_pressed) break;

            if (!opt_tuples_only) {
                // Record header spanning both columns
                if (opt_border == 2) {
                    fputs("\\hline\n", fout);
                    fprintf(fout, "\\multicolumn{2}{|c|}{\\textit{Record %lu}} \\\\\n", record++);
                } else {
                    fprintf(fout, "\\multicolumn{2}{c}{\\textit{Record %lu}} \\\\\n", record++);
                }
            }

            if (opt_border >= 1)
                fputs("\\hline\n", fout);
        }

        // Print field name and value
        latex_escaped_print(cont->headers[i % cont->ncolumns], fout);
        fputs(" & ", fout);
        latex_escaped_print(*ptr, fout);
        fputs(" \\\\\n", fout);
    }

    if (cont->opt->stop_table) {
        if (opt_border == 2)
            fputs("\\hline\n", fout);     // Bottom border

        fputs("\\end{tabular}\n\n\\noindent ", fout);

        // Print footers
        if (cont->footers && !opt_tuples_only && !cancel_pressed) {
            for (printTableFooter *f = cont->footers; f; f = f->next) {
                latex_escaped_print(f->data, fout);
                fputs(" \\\\\n", fout);
            }
        }
        fputc('\n', fout);
    }
}
```