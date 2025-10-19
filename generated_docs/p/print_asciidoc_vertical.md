# print_asciidoc_vertical

## Location
[src/fe_utils/print.c:2296-2391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2296-L2391)

## Overview
Prints table data in AsciiDoc vertical format where each record is displayed as a series of field-value pairs in a vertical layout.

## Definition

```c
static void
print_asciidoc_vertical(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function formats and outputs tabular data in AsciiDoc vertical format, where instead of displaying data in traditional columns and rows, each record is presented vertically with field names and their corresponding values. The function handles AsciiDoc-specific formatting including table headers, borders, cell alignment, and footer information. It supports various border styles (none, partial, full) and can optionally include record numbers and titles.

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing the table data, headers, formatting options, and metadata
- `*fout`: File stream where the formatted AsciiDoc output will be written
## Dependencies
- Functions called/Symbols referenced:
  - [asciidoc_escaped_print](../a/asciidoc_escaped_print.md) (for escaping special AsciiDoc characters in content)
  - [printTableContent](printTableContent.md) (data structure)
  - [printTableFooter](printTableFooter.md) (data structure)
  - cancel_pressed (global variable for interrupt handling)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- This is a static function within print.c, indicating it's used internally for AsciiDoc formatting
- Supports different border styles: 0 (no borders), 1 (no frame), 2 (full borders and grid)
- Handles cancellation via cancel_pressed global variable for responsive interruption
- Uses AsciiDoc table syntax with |==== delimiters and column specifications
- Record numbering starts from cont->opt->prior_records + 1 to support pagination
- Empty or whitespace-only cells are rendered as single space to maintain table structure
- Footers are displayed in a literal block (....)

## Simplified Source
```c
static void print_asciidoc_vertical(const printTableContent *cont, FILE *fout)
{
    bool opt_tuples_only = cont->opt->tuples_only;
    unsigned short opt_border = cont->opt->border;
    unsigned long record = cont->opt->prior_records + 1;
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

        // Create vertical table with header/value columns
        fputs("[cols=\"h,l\"", fout);

        // Add border styling
        if (opt_border == 0)
            fputs(",frame=\"none\",grid=\"none\"", fout);
        else if (opt_border == 1)
            fputs(",frame=\"none\"", fout);
        else
            fputs(",frame=\"all\",grid=\"all\"", fout);

        fputs("]\n|====\n", fout);
    }

    // Print each record as field-value pairs
    for (i = 0, ptr = cont->cells; *ptr; i++, ptr++) {
        // Start new record
        if (i % cont->ncolumns == 0) {
            if (cancel_pressed) break;

            if (!opt_tuples_only)
                fprintf(fout, "2+^|Record %lu\n", record++);
            else
                fputs("2+|\n", fout);
        }

        // Print field name
        fputs("<l|", fout);
        asciidoc_escaped_print(cont->headers[i % cont->ncolumns], fout);

        // Print field value with alignment
        fprintf(fout, " %s|", cont->aligns[i % cont->ncolumns] == 'r' ? ">l" : "<l");

        if ((*ptr)[strspn(*ptr, " \t")] == '\0')
            fputs(" ", fout);  // Empty cell
        else
            asciidoc_escaped_print(*ptr, fout);

        fputs("\n", fout);
    }

    fputs("|====\n", fout);

    // Print footers in literal block
    if (cont->opt->stop_table && !opt_tuples_only && cont->footers && !cancel_pressed) {
        fputs("\n....\n", fout);
        for (printTableFooter *f = cont->footers; f; f = f->next) {
            fprintf(fout, "%s\n", f->data);
        }
        fputs("....\n", fout);
    }
}
```