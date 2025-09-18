# printQueryOpt

## Location
src/include/fe_utils/print.h: 183 - 193

## Overview
The printQueryOpt struct extends printTableOpt with additional options specifically for printing query results, including null value handling, custom titles, footers, and internationalization support.

## Definition
```c
typedef struct printQueryOpt
{
    printTableOpt topt;             /* the options above */
    char       *nullPrint;          /* how to print null entities */
    char       *title;              /* override title */
    char      **footers;            /* override footer (default is "(xx rows)") */
    bool        translate_header;   /* do gettext on column headers */
    const bool *translate_columns;  /* translate_columns[i-1] => do gettext on col i */
    int         n_translate_columns; /* length of translate_columns[] */
} printQueryOpt;
```

## Detailed Description
The printQueryOpt structure is a specialized extension of printTableOpt designed specifically for query result formatting in PostgreSQL frontend utilities. It inherits all basic table formatting options while adding query-specific features such as null value representation, custom titles and footers, and comprehensive internationalization support. This structure is central to psql's display functionality, enabling flexible formatting of SQL query results with proper localization support. The structure supports column-specific translation control, allowing fine-grained control over which columns should be translated when displaying results in different languages.

## Parameters / Member Variables
- `topt`: Embedded printTableOpt structure containing base table formatting options
- `nullPrint`: String representation for NULL values in query results (e.g., "(null)", "", etc.)
- `title`: Optional custom title that overrides any default table title
- `footers`: Array of custom footer strings that replace the default "(xx rows)" footer
- `translate_header`: Boolean flag indicating whether column headers should be translated using gettext
- `translate_columns`: Array of boolean flags indicating which specific columns should be translated (indexed from 0)
- `n_translate_columns`: Length of the translate_columns array, specifying how many columns have translation settings

## Dependencies
- Functions called/Symbols referenced:
  - [printTableOpt](printTableOpt.md)
- Called from (representative examples):
  - [printQuery](printQuery.md)
  - [PrintQueryTuples](../P/PrintQueryTuples.md)
  - [PrintQueryResult](../P/PrintQueryResult.md)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md)
  - [printCrosstab](printCrosstab.md)
  - [describeAggregates](../d/describeAggregates.md)
  - [listAllDbs](../l/listAllDbs.md)
  - [listTables](../l/listTables.md)
  - [describeOneTableDetails](../d/describeOneTableDetails.md)

## Notes and Other Information
This structure is extensively used throughout psql and other PostgreSQL frontend tools for displaying query results with proper formatting and localization. The embedded printTableOpt provides access to all standard table formatting options (borders, alignment, output format), while the additional fields handle query-specific requirements. The translation features support PostgreSQL's internationalization efforts, allowing for localized display of query results while maintaining control over which elements should be translated. The structure is commonly found in psql's settings and is used by the printing subsystem to format all types of query output.