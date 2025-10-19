# print_csv_text

## Location
[src/fe_utils/print.c:1880-1919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L1880-L1919)

## Overview
Renders tabular data in standard CSV (Comma-Separated Values) format with proper field escaping and configurable separators, excluding titles and footers.

## Definition
```c
static void print_csv_text(const printTableContent *cont, FILE *fout)
```

## Detailed Description
This function outputs PostgreSQL query results in CSV format following RFC 4180 standards with PostgreSQL-specific adaptations. It handles the complete CSV rendering process including optional headers and row-by-row data output with proper field separation and line termination.

The function operates in two main phases:
1. **Header output**: When not in tuples-only mode and table start is enabled, it outputs column headers as the first CSV row, with each header field properly escaped using csv_print_field()
2. **Data output**: Processes the data cells sequentially, formatting each row with appropriate field separators and line breaks

Key features include:
- Configurable field separator via csvFieldSep option (typically comma)
- Proper field escaping for complex content through csv_print_field()
- System-dependent line ending handling (LF on Unix, CRLF on Windows)
- Tuples-only mode support that omits headers
- Integration with PostgreSQL's cancellation mechanism

The function deliberately excludes table titles and footers from CSV output to maintain data-only format suitable for import into spreadsheets and databases.

## Parameters / Member Variables
- `cont`: Pointer to printTableContent structure containing table data, headers, formatting options, and separator configuration
- `fout`: File stream for CSV output (typically stdout or a file)

## Dependencies
- Functions called/Symbols referenced:
  - [csv_print_field](../c/csv_print_field.md)
  - fputc (standard C library)
- Called from (representative examples):
  - [printTable](printTable.md)

## Notes and Other Information
This function is part of PostgreSQL's frontend utilities and is used by psql when outputting results in CSV format. The implementation prioritizes compatibility with standard CSV parsers while accommodating PostgreSQL-specific requirements. Line termination uses \n which gets converted to the appropriate system line ending in text mode, ensuring cross-platform compatibility. The function respects the global cancel_pressed flag for responsive interruption during large result set processing.

## Simplified Source
```c
static void print_csv_text(const printTableContent *cont, FILE *fout)
{
    const char *const *ptr;
    int i;

    if (cancel_pressed)
        return;

    // Print headers if not in tuples-only mode
    if (cont->opt->start_table && !cont->opt->tuples_only) {
        for (ptr = cont->headers; *ptr; ptr++) {
            if (ptr != cont->headers)
                fputc(cont->opt->csvFieldSep[0], fout);  // Add separator
            csv_print_field(*ptr, fout, cont->opt->csvFieldSep[0]);
        }
        fputc('\n', fout);  // End header row
    }

    // Print data cells row by row
    for (i = 0, ptr = cont->cells; *ptr; i++, ptr++) {
        csv_print_field(*ptr, fout, cont->opt->csvFieldSep[0]);

        // Add separator between columns, newline at end of row
        if ((i + 1) % cont->ncolumns)
            fputc(cont->opt->csvFieldSep[0], fout);
        else
            fputc('\n', fout);
    }
}
```