# print_aligned_vertical

## Location
[src/fe_utils/print.c:1324-1839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L1324-L1839)

## Overview
Prints tabular data in vertical format where each record is displayed with column names on the left and values on the right, with support for wrapping, borders, and multiline content.

## Definition
```c
static void print_aligned_vertical(const printTableContent *cont, FILE *fout, bool is_pager)
```

## Detailed Description
This function renders tabular data in a vertical layout format, commonly used in PostgreSQL when displaying results with `\x` (expanded display) mode. Each row of the table is presented as a set of key-value pairs, with column headers displayed vertically alongside their corresponding data values. The function handles complex formatting scenarios including:

- Multiple border styles (0, 1, 2) for different visual presentations
- Automatic text wrapping when content exceeds available width
- Multi-line content handling within cells
- Record numbering and header formatting
- Interactive pager integration for large result sets
- Proper spacing and alignment calculations based on terminal width

The function performs extensive width calculations to determine optimal formatting, considering header widths, data widths, border requirements, and available terminal space. It supports both wrapped and unwrapped modes, automatically adjusting layout based on content size and terminal constraints.

## Parameters / Member Variables
- `cont`: Pointer to printTableContent structure containing the table data, headers, formatting options, and display preferences
- `fout`: File stream for output (typically stdout or a pager)
- `is_pager`: Boolean indicating whether output is being sent to a pager program

## Dependencies
- Functions called/Symbols referenced:
  - [get_line_style](../g/get_line_style.md)
  - [footers_with_default](../f/footers_with_default.md)
  - [IsPagerNeeded](../I/IsPagerNeeded.md)
  - [pg_wcssize](pg_wcssize.md)
  - [pg_wcsformat](pg_wcsformat.md)
  - [print_aligned_vertical_line](print_aligned_vertical_line.md)
  - [strlen_max_width](../s/strlen_max_width.md)
  - [ClosePager](../C/ClosePager.md)
  - [pg_malloc](pg_malloc.md)
- Called from (representative examples):
  - [print_aligned_text](print_aligned_text.md)
  - [printTable](printTable.md)

## Notes and Other Information
This function is part of PostgreSQL's frontend utility library and is primarily used by psql for displaying query results in expanded format. The function handles complex edge cases like mixed multiline headers and data, terminal width detection via ioctl, and proper memory management for formatting structures. It respects various display options including tuples_only mode, column width limits, and environmental variables like COLUMNS for width detection.

## Simplified Source

```c
static void
print_aligned_vertical(const printTableContent *cont, FILE *fout, bool is_pager)
{
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    const printTextFormat *format = get_line_style(cont->opt);
    unsigned long record = cont->opt->prior_records + 1;
    unsigned int hwidth = 0, dwidth = 0;
    int output_columns = 0;

    if (cancel_pressed)
        return;

    // Handle empty result set
    if (cont->cells[0] == NULL)
    {
        if (!opt_tuples_only && cont->footers)
        {
            for (printTableFooter *f = cont->footers; f; f = f->next)
                fprintf(fout, "%s\n", f->data);
        }
        fputc('\n', fout);
        return;
    }

    // Find maximum header width
    for (int i = 0; i < cont->ncolumns; i++)
    {
        int width, height;
        pg_wcssize(cont->headers[i], strlen(cont->headers[i]),
                   encoding, &width, &height, NULL);
        if (width > hwidth)
            hwidth = width;
    }

    // Find maximum data width
    for (const char *const *ptr = cont->cells; *ptr; ptr++)
    {
        int width, height;
        pg_wcssize(*ptr, strlen(*ptr), encoding, &width, &height, NULL);
        if (width > dwidth)
            dwidth = width;
    }

    // Determine output width
    if (cont->opt->columns > 0)
        output_columns = cont->opt->columns;
    // Handle terminal width detection...

    // Calculate wrapping if needed
    if (cont->opt->format == PRINT_WRAPPED && output_columns > 0)
    {
        unsigned int total_width = hwidth + dwidth + 7; // Basic spacing
        if (total_width > output_columns)
            dwidth = output_columns - hwidth - 7; // Wrap data column
    }

    // Print title
    if (!opt_tuples_only && cont->title)
        fprintf(fout, "%s\n", cont->title);

    // Print each record
    for (int i = 0, ptr = cont->cells; *ptr; i++, ptr++)
    {
        if (cancel_pressed)
            break;

        // Print record separator (e.g., "-[ RECORD 1 ]-")
        if (i % cont->ncolumns == 0)
        {
            if (!opt_tuples_only)
                print_aligned_vertical_line(cont->opt, record++,
                                            hwidth, dwidth, output_columns,
                                            PRINT_RULE_TOP, fout);
        }

        // Print header | data pair
        const char *header = cont->headers[i % cont->ncolumns];
        const char *data = *ptr;

        if (opt_border >= 1)
            fprintf(fout, " ");

        // Print header (left-aligned)
        fprintf(fout, "%-*s", hwidth, header);

        if (opt_border >= 1)
            fprintf(fout, " | ");

        // Print data
        fprintf(fout, "%s", data);

        if (opt_border == 2)
            fprintf(fout, " ");

        fputc('\n', fout);
    }

    // Print footer
    if (cont->opt->stop_table)
    {
        if (opt_border == 2)
            print_aligned_vertical_line(cont->opt, 0, hwidth, dwidth,
                                        output_columns, PRINT_RULE_BOTTOM, fout);

        if (!opt_tuples_only && cont->footers)
        {
            for (printTableFooter *f = cont->footers; f; f = f->next)
                fprintf(fout, "%s\n", f->data);
        }

        fputc('\n', fout);
    }
}
```