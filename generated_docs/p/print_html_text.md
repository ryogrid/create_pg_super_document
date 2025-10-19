# print_html_text

## Location
[src/fe_utils/print.c:1993-2081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L1993-L2081)

## Overview
Renders table data in HTML format, generating a complete HTML table with headers, data cells, and optional footers for PostgreSQL query results.

## Definition

```c
static void
print_html_text(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function generates HTML table output from PostgreSQL query results stored in a printTableContent structure. It creates a properly formatted HTML table with configurable borders, alignment, and styling. The function handles the complete table lifecycle including opening table tags, headers, data rows, and closing tags with optional footers. It uses HTML escaping for all content to prevent HTML injection and ensures proper formatting. The function respects various output options like tuples-only mode and can handle cancellation during processing.

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing table data, headers, formatting options, and configuration
- `*fout`: Output file stream where the HTML table will be written
## Dependencies
- Functions called/Symbols referenced:
  - [html_escaped_print](../h/html_escaped_print.md) (for escaping HTML content)
  - [footers_with_default](../f/footers_with_default.md) (for retrieving table footers)
  - fprintf, fputs, fputc (standard C library functions)
  - strspn (standard C library function)
- Called from:
  - [printTable](printTable.md) (src/fe_utils/print.c:3503)

## Notes and Other Information
- Generates complete HTML table markup with configurable border and table attributes
- Handles optional table title as HTML caption element
- Creates table headers with center alignment in th elements
- Data cells use left or right alignment based on column alignment settings
- Empty or whitespace-only cells are rendered as '&nbsp;' to maintain table structure
- Supports tuples-only mode which omits headers and footers
- Includes cancellation support via cancel_pressed global variable
- Footers are rendered as paragraph elements with line breaks
- All text content is HTML-escaped to prevent markup injection

## Simplified Source

```c
static void
print_html_text(const printTableContent *cont, FILE *fout)
{
    // Extract options for easier access
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    const char *opt_table_attr = cont->opt->tableAttr;

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

        // Add column headers
        if (!opt_tuples_only) {
            fputs("  <tr>\n", fout);
            for (const char *const *ptr = cont->headers; *ptr; ptr++) {
                fputs("    <th align=\"center\">", fout);
                html_escaped_print(*ptr, fout);
                fputs("</th>\n", fout);
            }
            fputs("  </tr>\n", fout);
        }
    }

    // Print data cells row by row
    for (unsigned int i = 0, const char *const *ptr = cont->cells; *ptr; i++, ptr++) {
        // Start new row
        if (i % cont->ncolumns == 0) {
            if (cancel_pressed) break;
            fputs("  <tr valign=\"top\">\n", fout);
        }

        // Determine alignment and print cell
        const char *align = (cont->aligns[i % cont->ncolumns] == 'r') ? "right" : "left";
        fprintf(fout, "    <td align=\"%s\">", align);

        // Handle empty cells with non-breaking space
        if ((*ptr)[strspn(*ptr, " \t")] == '\0')
            fputs("&nbsp; ", fout);
        else
            html_escaped_print(*ptr, fout);

        fputs("</td>\n", fout);

        // Close row when complete
        if ((i + 1) % cont->ncolumns == 0)
            fputs("  </tr>\n", fout);
    }

    // Close table and add footers
    if (cont->opt->stop_table) {
        fputs("</table>\n", fout);

        // Print footers as paragraph
        if (!opt_tuples_only && !cancel_pressed) {
            printTableFooter *footers = footers_with_default(cont);
            if (footers) {
                fputs("<p>", fout);
                for (printTableFooter *f = footers; f; f = f->next) {
                    html_escaped_print(f->data, fout);
                    fputs("<br />\n", fout);
                }
                fputs("</p>", fout);
            }
        }
        fputc('\n', fout);
    }
}
```