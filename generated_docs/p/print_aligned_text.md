# print_aligned_text

## Location
[src/fe_utils/print.c:635-1224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L635-L1224)

## Overview
Renders tabular data in aligned text format with proper column borders, spacing, and text wrapping support for creating professional-looking table output.

## Definition

```c
struct lineptr **col_lineptrs;
```
## Detailed Description
This is the most sophisticated table formatting function in PostgreSQL's printing subsystem. It creates well-formatted tables with aligned columns, configurable borders (none, single, double), automatic text wrapping when content exceeds available width, and intelligent column width optimization. The function handles complex scenarios including multi-line cells, variable-width character encodings, automatic pager invocation for large output, and responsive formatting that adapts to terminal width. It supports both horizontal and vertical layout modes, with automatic switching to vertical mode when the table is too wide for the available display width.

## Parameters / Member Variables
- : Pointer to printTableContent structure containing table data, headers, alignment settings, and formatting options
- : FILE pointer to the output stream where the formatted table will be written
- : Boolean indicating whether output is going to a pager (affects width calculations and formatting decisions)

## Dependencies
- Functions called/Symbols referenced:
  - [get_line_style](../g/get_line_style.md) (gets formatting characters for the current line style)
  - [_print_horizontal_line](_print_horizontal_line.md) (draws horizontal border lines)
  - [pg_wcssize](pg_wcssize.md) (calculates display width of wide character strings)
  - [pg_wcsformat](pg_wcsformat.md) (formats wide character strings for display)
  - [strlen_max_width](../s/strlen_max_width.md) (calculates byte length up to a display width limit)
  - [print_aligned_vertical](print_aligned_vertical.md) (alternative vertical layout for wide tables)
  - [PageOutput](../P/PageOutput.md) (initiates pager for large output)
  - [IsPagerNeeded](../I/IsPagerNeeded.md) (determines if pager is required)
  - [footers_with_default](../f/footers_with_default.md) (gets table footers with defaults)
  - [pg_malloc0](pg_malloc0.md)/pg_malloc (memory allocation functions)
  - Various PRINT_RULE_* and PRINT_LINE_WRAP_* constants for formatting states
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- Most complex function in the PostgreSQL table formatting system with ~590 lines of code
- Implements intelligent column width optimization that shrinks columns with high max/average width ratios
- Supports automatic text wrapping with configurable wrap points and visual indicators
- Handles multi-byte character encodings correctly for international text display
- Automatically invokes pager when output exceeds terminal dimensions
- Can switch to vertical layout mode (print_aligned_vertical) when table is too wide
- Supports three border styles: 0=none, 1=simple, 2=full borders with corner characters
- Handles column alignment (left/right) specified in cont->aligns array
- Processes embedded newlines in cell data and creates multi-line table rows
- Uses sophisticated memory management with multiple dynamically allocated arrays
- Function is static, indicating it's only used within the print.c module
- Essential for creating the professional table output that PostgreSQL is known for in psql client

## Simplified Source

```c
static void
print_aligned_text(const printTableContent *cont, FILE *fout, bool is_pager)
{
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    const printTextFormat *format = get_line_style(cont->opt);
    unsigned int col_count = cont->ncolumns;
    unsigned int *width_header, *max_width, *width_wrap;
    unsigned int width_total;
    int output_columns = 0;

    if (cancel_pressed)
        return;

    // Allocate arrays for column width calculations
    width_header = pg_malloc0(col_count * sizeof(*width_header));
    max_width = pg_malloc0(col_count * sizeof(*max_width));
    width_wrap = pg_malloc0(col_count * sizeof(*width_wrap));

    // Scan headers to find maximum widths
    for (int i = 0; i < col_count; i++)
    {
        int width, nl_lines, bytes_required;
        pg_wcssize(cont->headers[i], strlen(cont->headers[i]),
                   encoding, &width, &nl_lines, &bytes_required);
        if (width > max_width[i])
            max_width[i] = width;
        width_header[i] = width;
    }

    // Scan all cells to find maximum widths
    for (const char *const *ptr = cont->cells; *ptr; ptr++)
    {
        int width, nl_lines, bytes_required;
        pg_wcssize(*ptr, strlen(*ptr), encoding,
                   &width, &nl_lines, &bytes_required);
        if (width > max_width[i % col_count])
            max_width[i % col_count] = width;
    }

    // Calculate total width needed
    width_total = (opt_border == 0) ? col_count :
                  (opt_border == 1) ? col_count * 3 - 1 : col_count * 3 + 1;
    for (int i = 0; i < col_count; i++)
        width_total += max_width[i];

    // Set wrap widths and determine output columns
    for (int i = 0; i < col_count; i++)
        width_wrap[i] = max_width[i];

    if (cont->opt->columns > 0)
        output_columns = cont->opt->columns;
    // Handle terminal width detection...

    // Switch to vertical mode if table too wide
    if (cont->opt->expanded == 2 && output_columns > 0 &&
        cont->ncolumns > 1 && width_total > output_columns)
    {
        print_aligned_vertical(cont, fout, is_pager);
        goto cleanup;
    }

    // Print title if present
    if (cont->title && !opt_tuples_only)
        fprintf(fout, "%s\n", cont->title);

    // Print headers with borders
    if (!opt_tuples_only)
    {
        if (opt_border == 2)
            _print_horizontal_line(col_count, width_wrap, opt_border,
                                   PRINT_RULE_TOP, format, fout);

        // Print header row with proper formatting...
        _print_horizontal_line(col_count, width_wrap, opt_border,
                               PRINT_RULE_MIDDLE, format, fout);
    }

    // Print data rows
    for (int i = 0, ptr = cont->cells; *ptr; i += col_count, ptr += col_count)
    {
        if (cancel_pressed)
            break;

        // Format and print each cell in the row
        for (int j = 0; j < col_count; j++)
        {
            // Handle borders, alignment, and wrapping
            if (opt_border >= 1)
                fputs(" ", fout);

            // Print cell content with proper alignment
            if (cont->aligns[j] == 'r')
                fprintf(fout, "%*s", width_wrap[j], ptr[j]);
            else
                fprintf(fout, "%-*s", width_wrap[j], ptr[j]);

            if (j < col_count - 1 && opt_border >= 1)
                fputs(" | ", fout);
        }
        fputc('\n', fout);
    }

    // Print footer
    if (cont->opt->stop_table)
    {
        if (opt_border == 2)
            _print_horizontal_line(col_count, width_wrap, opt_border,
                                   PRINT_RULE_BOTTOM, format, fout);
        fputc('\n', fout);
    }

cleanup:
    // Free allocated memory
    free(width_header);
    free(max_width);
    free(width_wrap);
}
```