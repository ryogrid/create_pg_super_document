# printTableInit

## Location
[src/fe_utils/print.c:3172-3219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3172-L3219)

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
  - [printTableOpt](printTableOpt.md) (options structure type)
  - [printTableContent](printTableContent.md) (content structure type)
  - [pg_malloc0](pg_malloc0.md) (PostgreSQL's zero-initialized malloc)
  - EXIT_FAILURE (standard exit code for failure)
- Called from (representative examples):
  - [printCrosstab](printCrosstab.md) (src/bin/psql/crosstabview.c:299)
  - [describeOneTableDetails](../d/describeOneTableDetails.md) (src/bin/psql/describe.c:2050)
  - [describeRoles](../d/describeRoles.md) (src/bin/psql/describe.c:3670)
  - [describePublications](../d/describePublications.md) (src/bin/psql/describe.c:6429)
  - [printQuery](printQuery.md) (src/fe_utils/print.c:3560)

## Notes and Other Information
- Must be called before any other printTable methods are used on the content structure
- Requires printTableCleanup to be called when done to free allocated memory
- Performs overflow protection when calculating total cells (ncolumns * nrows)
- Exits with failure if table size would cause integer overflow in memory allocation
- Initializes cellmustfree and footers to NULL initially
- Sets up current position pointers (header, cell, footer, align) for iterative content addition
- Uses pg_malloc0 for zero-initialized memory allocation

## Simplified Source

```c
void
printTableInit(printTableContent *const content, const printTableOpt *opt,
               const char *title, const int ncolumns, const int nrows)
{
    // Set basic table parameters
    content->opt = opt;
    content->title = title;  // Not duplicated - caller must maintain
    content->ncolumns = ncolumns;
    content->nrows = nrows;

    // Allocate memory for headers array
    content->headers = pg_malloc0((ncolumns + 1) * sizeof(*content->headers));

    // Calculate total cells with overflow protection
    uint64 total_cells = (uint64) ncolumns * nrows;
    if (total_cells >= SIZE_MAX / sizeof(*content->cells)) {
        fprintf(stderr, _("Cannot print table contents: number of cells %lld is equal to or exceeds maximum %lld.\n"),
                (long long int) total_cells,
                (long long int) (SIZE_MAX / sizeof(*content->cells)));
        exit(EXIT_FAILURE);
    }

    // Allocate memory for cells array
    content->cells = pg_malloc0((total_cells + 1) * sizeof(*content->cells));

    // Initialize optional arrays to NULL
    content->cellmustfree = NULL;
    content->footers = NULL;

    // Allocate memory for column alignment settings
    content->aligns = pg_malloc0((ncolumns + 1) * sizeof(*content->align));

    // Set up current position pointers for adding content
    content->header = content->headers;
    content->cell = content->cells;
    content->footer = content->footers;
    content->align = content->aligns;
    content->cellsadded = 0;
}
```