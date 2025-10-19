# print_asciidoc_text

## Location
[src/fe_utils/print.c:2186-2295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2186-L2295)

## Overview
Renders table data in AsciiDoc table format, generating properly formatted AsciiDoc markup with configurable borders, column alignment, and headers for PostgreSQL query results.

## Definition

```c
static void
print_asciidoc_text(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function generates AsciiDoc table output from PostgreSQL query results stored in a printTableContent structure. It creates properly formatted AsciiDoc tables using the standard table block syntax with pipe delimiters and table attributes. The function handles the complete table lifecycle including table definition blocks, headers, data rows, and footers. It supports various border styles, column alignments, and formatting options. The function uses AsciiDoc-specific escaping for content and implements AsciiDoc's table formatting conventions including column specifications, frame options, and grid settings.

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing table data, headers, formatting options, and configuration
- `*fout`: Output file stream where the AsciiDoc table will be written
## Dependencies
- Functions called/Symbols referenced:
  - [asciidoc_escaped_print](../a/asciidoc_escaped_print.md) (for escaping AsciiDoc content)
  - [footers_with_default](../f/footers_with_default.md) (for retrieving table footers)
  - fprintf, fputs (standard C library functions)
  - strspn (standard C library function)
- Called from:
  - [printTable](printTable.md) (src/fe_utils/print.c:3509)

## Notes and Other Information
- Uses AsciiDoc table block syntax with |==== delimiters
- Generates proper column specifications with alignment indicators (<l for left, >l for right)
- Supports different border styles via frame and grid attributes (none, frame-only, or full)
- Headers use ^l| prefix for center alignment when not in tuples-only mode
- Creates table title using AsciiDoc's title syntax (leading dot)
- Empty cells are handled by outputting just the pipe delimiter with spacing
- All content is AsciiDoc-escaped to prevent formatting conflicts
- Footers are rendered in literal blocks using .... delimiters
- Includes cancellation support during processing
- Enforces proper AsciiDoc spacing and formatting conventions
- Table definition includes header option when headers are present

## Simplified Source
```c
static void print_asciidoc_text(const printTableContent *cont, FILE *fout)
{
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    unsigned int i;
    const char *const *ptr;

    if (cancel_pressed)
        return;

    if (cont->opt->start_table) {
        fputs("\n", fout);  // Start new paragraph

        // Print title if present
        if (!opt_tuples_only && cont->title) {
            fprintf(fout, ".%s\n", cont->title);
        }

        // Generate table definition with column specs
        fprintf(fout, "[%scols=\"", !opt_tuples_only ? "options=\"header\"," : "");
        for (i = 0; i < cont->ncolumns; i++) {
            if (i != 0) fputs(",", fout);
            fputs(cont->aligns[i % cont->ncolumns] == 'r' ? ">l" : "<l", fout);
        }
        fputs("\"", fout);

        // Add border styling based on border option
        if (opt_border == 0)
            fputs(",frame=\"none\",grid=\"none\"", fout);
        else if (opt_border == 1)
            fputs(",frame=\"none\"", fout);
        else
            fputs(",frame=\"all\",grid=\"all\"", fout);

        fputs("]\n|====\n", fout);

        // Print headers
        if (!opt_tuples_only) {
            for (ptr = cont->headers; *ptr; ptr++) {
                if (ptr != cont->headers) fputs(" ", fout);
                fputs("^l|", fout);
                asciidoc_escaped_print(*ptr, fout);
            }
            fputs("\n", fout);
        }
    }

    // Print data cells
    for (i = 0, ptr = cont->cells; *ptr; i++, ptr++) {
        if (i % cont->ncolumns == 0 && cancel_pressed)
            break;

        if (i % cont->ncolumns != 0) fputs(" ", fout);
        fputs("|", fout);

        // Handle empty cells vs content
        if ((*ptr)[strspn(*ptr, " \t")] == '\0') {
            if ((i + 1) % cont->ncolumns != 0) fputs(" ", fout);
        } else {
            asciidoc_escaped_print(*ptr, fout);
        }

        if ((i + 1) % cont->ncolumns == 0) fputs("\n", fout);
    }

    fputs("|====\n", fout);

    // Print footers in literal block
    if (cont->opt->stop_table && !opt_tuples_only && !cancel_pressed) {
        printTableFooter *footers = footers_with_default(cont);
        if (footers) {
            fputs("\n....\n", fout);
            for (printTableFooter *f = footers; f; f = f->next) {
                fprintf(fout, "%s\n", f->data);
            }
            fputs("....\n", fout);
        }
    }
}
```