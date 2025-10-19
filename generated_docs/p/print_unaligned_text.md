# print_unaligned_text

## Location
[src/fe_utils/print.c:422-512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L422-L512)

## Overview
Prints table data in unaligned text format where fields are separated by configurable delimiters without column alignment or borders.

## Definition

```c
static void
print_unaligned_text(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function renders tabular data in a simple unaligned text format, primarily used for machine-readable output or when visual alignment is not required. The function processes the table content sequentially, printing the title, headers, data cells, and footers separated by configurable field and record separators. It supports both human-readable and machine-readable output modes through the tuples_only option, and handles special cases like zero-byte record separators for compatibility with Unix tools like find -print0 and xargs.

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing the table data, headers, options, and formatting settings
- `*fout`: FILE pointer to the output stream where the formatted text will be written
## Dependencies
- Functions called/Symbols referenced:
  - [print_separator](print_separator.md) (for field and record separation)
  - [footers_with_default](../f/footers_with_default.md) (to get default footers if needed)
  - fputs (standard C library function for string output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- Uses configurable field separators (fieldSep) between columns and record separators (recordSep) between rows
- Respects the tuples_only option to suppress headers, title, and footers for machine-readable output
- Handles cancellation through the global cancel_pressed variable for responsive user interaction
- Special handling for zero-byte record separators to maintain compatibility with Unix pipeline tools
- The last record is always terminated with a newline unless using zero-byte record separator mode
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem

## Simplified Source

```c
static void print_unaligned_text(const printTableContent *cont, FILE *fout) {
    bool opt_tuples_only = cont->opt->tuples_only;
    bool need_recordsep = false;

    if (cancel_pressed) return;

    // Print title and headers if starting table
    if (cont->opt->start_table) {
        if (!opt_tuples_only && cont->title) {
            fputs(cont->title, fout);
            print_separator(cont->opt->recordSep, fout);
        }

        // Print column headers with field separators
        if (!opt_tuples_only) {
            for (const char *const *ptr = cont->headers; *ptr; ptr++) {
                if (ptr != cont->headers) {
                    print_separator(cont->opt->fieldSep, fout);
                }
                fputs(*ptr, fout);
            }
            need_recordsep = true;
        }
    } else {
        need_recordsep = true;
    }

    // Print data cells
    for (unsigned int i = 0; const char *const *ptr = cont->cells; *ptr; i++, ptr++) {
        if (need_recordsep) {
            print_separator(cont->opt->recordSep, fout);
            need_recordsep = false;
            if (cancel_pressed) break;
        }
        fputs(*ptr, fout);

        // Add field separator between columns, record separator at row end
        if ((i + 1) % cont->ncolumns) {
            print_separator(cont->opt->fieldSep, fout);
        } else {
            need_recordsep = true;
        }
    }

    // Print footers if stopping table
    if (cont->opt->stop_table) {
        printTableFooter *footers = footers_with_default(cont);
        if (!opt_tuples_only && footers && !cancel_pressed) {
            for (printTableFooter *f = footers; f; f = f->next) {
                if (need_recordsep) {
                    print_separator(cont->opt->recordSep, fout);
                    need_recordsep = false;
                }
                fputs(f->data, fout);
                need_recordsep = true;
            }
        }

        // Final record termination
        if (need_recordsep) {
            if (cont->opt->recordSep.separator_zero) {
                print_separator(cont->opt->recordSep, fout);
            } else {
                fputc('\n', fout);
            }
        }
    }
}
```