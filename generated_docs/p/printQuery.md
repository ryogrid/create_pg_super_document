# printQuery

## Location
[src/fe_utils/print.c:3549-3613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3549-L3613)

## Overview
The printQuery function processes PostgreSQL query results and formats them for display, converting PGresult data into a printable table format with proper column headers, cell values, and optional footers.

## Definition

```c
void
printQuery(const PGresult *result, const printQueryOpt *opt,
		   FILE *fout, bool is_pager, FILE *flog)
```
## Detailed Description
This function serves as the bridge between PostgreSQL query result data (PGresult) and the table printing infrastructure. It extracts data from a PGresult structure and constructs a printTableContent object that can be passed to printTable for formatted output. The function handles column headers by extracting field names from the result set, determines appropriate column alignment based on data types, processes cell values including NULL handling and numeric locale formatting, and manages optional translation of column content and headers.

The function iterates through all rows and columns of the result set, applying locale-specific numeric formatting for right-aligned numeric columns when enabled. It also supports column content translation and custom NULL value representation. After populating the table content structure, it calls printTable to handle the actual output formatting and cleanup.

## Parameters / Member Variables
- `*result`: Pointer to PGresult structure containing the query result data with rows, columns, and metadata
- `*opt`: Pointer to printQueryOpt structure containing formatting options, translation settings, null print string, title, and footers
- `*fout`: File pointer for the primary output destination (stdout, file, or pager pipe)
- `is_pager`: Boolean indicating whether the caller has already set up fout as a pager pipe
- `*flog`: Optional file pointer for simultaneous logging output (used with --log-file option)
## Dependencies
- Functions called/Symbols referenced:
  - [printTableInit](printTableInit.md) (initialize table content structure)
  - [PQnfields](../P/PQnfields.md) (get number of columns in result)
  - [PQntuples](../P/PQntuples.md) (get number of rows in result) 
  - [printTableAddHeader](printTableAddHeader.md) (add column headers)
  - [PQfname](../P/PQfname.md) (get field name for column)
  - [column_type_alignment](../c/column_type_alignment.md) (determine column alignment from data type)
  - [PQftype](../P/PQftype.md) (get data type OID for column)
  - [PQgetisnull](../P/PQgetisnull.md) (check if cell value is NULL)
  - [PQgetvalue](../P/PQgetvalue.md) (get cell value as string)
  - [format_numeric_locale](../f/format_numeric_locale.md) (apply locale formatting to numeric values)
  - [printTableAddCell](printTableAddCell.md) (add cell content to table)
  - [printTableAddFooter](printTableAddFooter.md) (add footer text)
  - [printTable](printTable.md) (output formatted table)
  - [printTableCleanup](printTableCleanup.md) (free table content memory)
- Called from (representative examples):
  - [PrintQueryTuples](../P/PrintQueryTuples.md) (main query output in psql)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (query execution results)
  - [describeAggregates](../d/describeAggregates.md) (aggregate function descriptions)
  - [listAllDbs](../l/listAllDbs.md) (database listings)
  - [listTables](../l/listTables.md) (table listings)

## Notes and Other Information
- The function gracefully handles cancellation by checking cancel_pressed at the start
- Column alignment is automatically determined based on PostgreSQL data types through column_type_alignment
- [Numeric](../N/Numeric.md) locale formatting is applied only to right-aligned columns when the numericLocale option is enabled
- The translate_columns array allows selective translation of specific columns, with proper bounds checking via assertions
- NULL values are handled specially, using either the custom nullPrint string or an empty string
- Memory management is handled carefully with mustfree flags for dynamically allocated formatted strings
- The function supports both column content translation and header translation independently
- Footer text can be added through the opt->footers array for additional context or summary information

## Simplified Source

```c
void printQuery(const PGresult *result, const printQueryOpt *opt,
               FILE *fout, bool is_pager, FILE *flog) {
    printTableContent cont;
    int i, r, c;

    if (cancel_pressed)
        return;

    // Initialize table with result dimensions
    printTableInit(&cont, &opt->topt, opt->title,
                   PQnfields(result), PQntuples(result));

    // Add column headers with proper alignment
    for (i = 0; i < cont.ncolumns; i++) {
        printTableAddHeader(&cont, PQfname(result, i),
                           opt->translate_header,
                           column_type_alignment(PQftype(result, i)));
    }

    // Fill table cells
    for (r = 0; r < cont.nrows; r++) {
        for (c = 0; c < cont.ncolumns; c++) {
            char *cell;
            bool mustfree = false;
            bool translate;

            // Handle NULL values
            if (PQgetisnull(result, r, c)) {
                cell = opt->nullPrint ? opt->nullPrint : "";
            } else {
                cell = PQgetvalue(result, r, c);

                // Apply numeric locale formatting for right-aligned columns
                if (cont.aligns[c] == 'r' && opt->topt.numericLocale) {
                    cell = format_numeric_locale(cell);
                    mustfree = true;
                }
            }

            translate = (opt->translate_columns && opt->translate_columns[c]);
            printTableAddCell(&cont, cell, translate, mustfree);
        }
    }

    // Add footers if provided
    if (opt->footers) {
        for (char **footer = opt->footers; *footer; footer++)
            printTableAddFooter(&cont, *footer);
    }

    // Print table and cleanup
    printTable(&cont, fout, is_pager, flog);
    printTableCleanup(&cont);
}
```