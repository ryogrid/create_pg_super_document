# printTableInit

## Location
src/fe_utils/print.c: 3172 - 3219

## Overview
Initializes a printTableContent structure for table printing, allocating memory for headers, cells, and alignment information.

## Definition
```c
void printTableInit(printTableContent *const content, const printTableOpt *opt,
                   const char *title, const int ncolumns, const int nrows)
```

## Detailed Description
This function initializes a printTableContent structure that will be used to store and format tabular data for output. It sets up the basic table parameters, allocates memory for the headers array, cells array, and alignment array. The function performs overflow checking when calculating the total number of cells to prevent memory allocation issues with very large tables. It initializes all pointer fields and sets up the current position pointers for adding content. The title string is not duplicated, so the caller must ensure its availability throughout the table's lifetime.

## Parameters / Member Variables
- `content`: Pointer to the printTableContent structure to initialize
- `opt`: Pointer to printTableOpt structure containing formatting options
- `title`: Table title string (not duplicated - caller must maintain)
- `ncolumns`: Number of columns in the table
- `nrows`: Number of rows in the table

## Dependencies
- Functions called/Symbols referenced:
  - printTableOpt (options structure type)
  - printTableContent (content structure type)
  - pg_malloc0 (PostgreSQL's zero-initialized malloc)
  - EXIT_FAILURE (standard exit code for failure)
- Called from (representative examples):
  - printCrosstab (src/bin/psql/crosstabview.c:299)
  - describeOneTableDetails (src/bin/psql/describe.c:2050)
  - describeRoles (src/bin/psql/describe.c:3670)
  - describePublications (src/bin/psql/describe.c:6429)
  - printQuery (src/fe_utils/print.c:3560)

## Notes and Other Information
- Must be called before any other printTable methods are used on the content structure
- Requires printTableCleanup to be called when done to free allocated memory
- Performs overflow protection when calculating total cells (ncolumns * nrows)
- Exits with failure if table size would cause integer overflow in memory allocation
- Initializes cellmustfree and footers to NULL initially
- Sets up current position pointers (header, cell, footer, align) for iterative content addition
- Uses pg_malloc0 for zero-initialized memory allocation