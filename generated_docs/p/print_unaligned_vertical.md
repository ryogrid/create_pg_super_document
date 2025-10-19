# print_unaligned_vertical

## Location
[src/fe_utils/print.c:513-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L513-L592)

## Overview
Prints table data in unaligned vertical format where each field appears on its own line with the column header, creating a record-oriented output.

## Definition

```c
static void
print_unaligned_vertical(const printTableContent *cont, FILE *fout)
```
## Detailed Description
This function renders tabular data in a vertical (record-oriented) format where each row is displayed as a series of "header: value" pairs, with each field on its own line. This format is particularly useful for displaying wide tables or when you want to see each record's complete information in a readable vertical layout. The function uses double record separators to distinguish between different table records and includes support for titles and footers when not in tuples-only mode.

## Parameters / Member Variables
- `*cont`: Pointer to printTableContent structure containing the table data, headers, options, and formatting settings
- `*fout`: FILE pointer to the output stream where the formatted text will be written
## Dependencies
- Functions called/Symbols referenced:
  - [print_separator](print_separator.md) (for field and record separation)
  - fputs (standard C library function for string output)
  - fputc (standard C library function for character output)
- Called from (representative examples):
  - [printTable](printTable.md) (main table printing dispatcher function)

## Notes and Other Information
- Uses vertical format where each field appears as "header<fieldSep>value" on its own line
- Record separators appear twice between records to visually separate different rows
- Within each record, individual field lines are separated by single record separators
- Respects the tuples_only option to suppress title and footers for machine-readable output
- Handles cancellation through the global cancel_pressed variable for responsive user interaction
- Special handling for zero-byte record separators to maintain compatibility with Unix pipeline tools
- The last record is terminated with a newline unless using zero-byte record separator mode
- Function is static, indicating it's only used within the print.c module as part of the table formatting subsystem
- Particularly useful for displaying results with many columns or when each record needs detailed inspection

## Simplified Source

```c
static void print_unaligned_vertical(const printTableContent *cont, FILE *fout) {
    bool opt_tuples_only = cont->opt->tuples_only;
    bool need_recordsep = false;

    if (cancel_pressed) return;

    // Print title if starting table
    if (cont->opt->start_table) {
        if (!opt_tuples_only && cont->title) {
            fputs(cont->title, fout);
            need_recordsep = true;
        }
    } else {
        need_recordsep = true;
    }

    // Print records in vertical format (header: value per line)
    for (unsigned int i = 0; const char *const *ptr = cont->cells; *ptr; i++, ptr++) {
        if (need_recordsep) {
            // Double record separator to separate records
            print_separator(cont->opt->recordSep, fout);
            print_separator(cont->opt->recordSep, fout);
            need_recordsep = false;
            if (cancel_pressed) break;
        }

        // Print "header: value" format
        fputs(cont->headers[i % cont->ncolumns], fout);
        print_separator(cont->opt->fieldSep, fout);
        fputs(*ptr, fout);

        // Single record separator between fields, double for next record
        if ((i + 1) % cont->ncolumns) {
            print_separator(cont->opt->recordSep, fout);
        } else {
            need_recordsep = true;
        }
    }

    // Print footers if stopping table
    if (cont->opt->stop_table) {
        if (!opt_tuples_only && cont->footers && !cancel_pressed) {
            print_separator(cont->opt->recordSep, fout);
            for (printTableFooter *f = cont->footers; f; f = f->next) {
                print_separator(cont->opt->recordSep, fout);
                fputs(f->data, fout);
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