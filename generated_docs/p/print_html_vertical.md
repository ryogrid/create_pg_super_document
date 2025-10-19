# print_html_vertical

## Location
[src/fe_utils/print.c:2082-2167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2082-L2167)

## Overview
Renders table data in vertical HTML format where each row is displayed as a series of field-value pairs, useful for displaying detailed record information.

## Definition

```c
static void
print_html_vertical(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function generates HTML output in vertical format, displaying table data as field-value pairs rather than traditional tabular rows and columns. Each record is presented with its fields listed vertically, where column headers become row labels and the corresponding data values are displayed alongside them. This format is particularly useful for displaying detailed information about individual records or when dealing with tables that have many columns that would be difficult to read in traditional horizontal format. The function includes record numbering and handles all HTML escaping to prevent injection attacks.

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing table data, headers, formatting options, and configuration
- `*fout`: Output file stream where the vertical HTML table will be written
## Dependencies
- Functions called/Symbols referenced:
  - [html_escaped_print](../h/html_escaped_print.md) (for escaping HTML content)
  - fprintf, fputs, fputc (standard C library functions)
  - strspn (standard C library function)
- Called from:
  - [printTable](printTable.md) (src/fe_utils/print.c:3501)

## Notes and Other Information
- Displays data in vertical format with field names in header cells and values in data cells
- Each record is numbered and separated by a spanning row header (unless in tuples-only mode)
- Creates two-column layout where first column contains field names, second contains values
- Each field-value pair gets its own table row
- Record separators use colspan=2 to span the full table width
- Empty or whitespace-only values are rendered as '&nbsp;' to maintain visual structure
- Supports tuples-only mode which omits record numbers and uses empty separators
- All content is HTML-escaped to prevent markup injection
- Includes cancellation support during processing
- Uses same alignment settings as horizontal mode for data values
- Footers are rendered as paragraph elements when table processing completes

## Simplified Source

```c
static void
print_html_vertical(const printTableContent *cont, FILE *fout)
{
    // Extract options for easier access
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    const char *opt_table_attr = cont->opt->tableAttr;
    unsigned long record = cont->opt->prior_records + 1;

    if (cancel_pressed)
        return;

    // Start table if needed
    if (cont->opt->start_table) {
        // Print opening table tag with border and attributes
        fprintf(fout, "<table border=\"%d\"", opt_border);
        if (opt_table_attr)
            fprintf(fout, " %s", opt_table_attr);
        fputs(">\n", fout);

        // Add title as caption
        if (!opt_tuples_only && cont->title) {
            fputs("  <caption>", fout);
            html_escaped_print(cont->title, fout);
            fputs("</caption>\n", fout);
        }
    }

    // Print records vertically (each field becomes a row)
    for (unsigned int i = 0, const char *const *ptr = cont->cells; *ptr; i++, ptr++) {
        // Start new record separator
        if (i % cont->ncolumns == 0) {
            if (cancel_pressed) break;

            if (!opt_tuples_only)
                fprintf(fout, "\n  <tr><td colspan=\"2\" align=\"center\">Record %lu</td></tr>\n", record++);
            else
                fputs("\n  <tr><td colspan=\"2\">&nbsp;</td></tr>\n", fout);
        }

        // Print field name and value as separate table row
        fputs("  <tr valign=\"top\">\n    <th>", fout);
        html_escaped_print(cont->headers[i % cont->ncolumns], fout);
        fputs("</th>\n", fout);

        // Print data cell with alignment
        const char *align = (cont->aligns[i % cont->ncolumns] == 'r') ? "right" : "left";
        fprintf(fout, "    <td align=\"%s\">", align);

        // Handle empty cells with non-breaking space
        if ((*ptr)[strspn(*ptr, " \t")] == '\0')
            fputs("&nbsp; ", fout);
        else
            html_escaped_print(*ptr, fout);

        fputs("</td>\n  </tr>\n", fout);
    }

    // Close table and add footers
    if (cont->opt->stop_table) {
        fputs("</table>\n", fout);

        // Print footers as paragraph
        if (!opt_tuples_only && cont->footers && !cancel_pressed) {
            fputs("<p>", fout);
            for (printTableFooter *f = cont->footers; f; f = f->next) {
                html_escaped_print(f->data, fout);
                fputs("<br />\n", fout);
            }
            fputs("</p>", fout);
        }
        fputc('\n', fout);
    }
}
```