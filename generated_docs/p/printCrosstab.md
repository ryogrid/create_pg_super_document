# printCrosstab

## Location
[src/bin/psql/crosstabview.c:286-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L286-L437)

## Overview
Internal function that renders the actual cross-tabulated output by formatting and displaying the pivoted data using PostgreSQL's printTable* functions.

## Definition
```c
static bool printCrosstab(const PGresult *result,
                         int num_columns, pivot_field *piv_columns, int field_for_columns,
                         int num_rows, pivot_field *piv_rows, int field_for_rows,
                         int field_for_data)
```

## Detailed Description
printCrosstab is the core rendering function for the crosstab view feature in psql. It takes processed pivot data and generates a formatted table output using PostgreSQL's standard table printing infrastructure. The function operates in three main phases:

1. **Table Setup and Headers**: Initializes the output table structure and sets up column headers, including the unchanged first column and dynamically generated horizontal headers from pivot data.

2. **Row Headers**: Populates the vertical headers (row names) in the first column using the processed row pivot data.

3. **Data Population**: Iterates through the original result set to place data values into the correct cross-tabulated positions, using binary search to efficiently locate the appropriate row/column intersections.

The function includes validation to ensure each cell receives at most one data value, reporting errors for duplicate entries that would create ambiguous crosstab results.

## Parameters / Member Variables
- `*result`: The original PGresult containing the query data to be cross-tabulated
- `num_columns`: Number of distinct values in the horizontal header (columns)
- `*piv_columns`: Array of pivot_field structures representing column headers, sorted by rank
- `field_for_columns`: Column index in the result set used for horizontal headers
- `num_rows`: Number of distinct values in the vertical header (rows)
- `*piv_rows`: Array of pivot_field structures representing row headers, sorted by rank
- `field_for_rows`: Column index in the result set used for vertical headers
- `field_for_data`: Column index in the result set containing the data values to display
## Dependencies
- Functions called/Symbols referenced:
  - [printTableInit](printTableInit.md), printTableAddHeader, printTable, printTableCleanup
  - [PQfname](../P/PQfname.md), PQftype, PQgetisnull, PQgetvalue, PQntuples
  - [column_type_alignment](../c/column_type_alignment.md), pivotFieldCompare
  - bsearch, pg_malloc, pg_free, pg_log_error
- Called from (representative examples):
  - [PrintResultInCrosstab](../P/PrintResultInCrosstab.md) (src/bin/psql/crosstabview.c:267)

## Notes and Other Information
- Uses binary search for efficient lookup of row and column positions during data placement
- Creates a reverse mapping array (horiz_map) to iterate over columns in rank order without O(N²) complexity
- Handles NULL values appropriately by using the configured nullPrint option
- Validates data uniqueness per cell to prevent ambiguous crosstab results
- Manages memory allocation for temporary structures and ensures proper cleanup
- Returns false on error conditions (like duplicate data values), true on success
- All uninitialized cells are set to empty strings before final table rendering

## Simplified Source

```c
static bool printCrosstab(const PGresult *result,
                         int num_columns, pivot_field *piv_columns, int field_for_columns,
                         int num_rows, pivot_field *piv_rows, int field_for_rows,
                         int field_for_data) {
    printQueryOpt popt = pset.popt;
    printTableContent cont;
    bool success = false;

    // Initialize table structure
    printTableInit(&cont, &popt.topt, popt.title, num_columns + 1, num_rows);

    // Set up column headers
    // First column: unchanged row header name
    printTableAddHeader(&cont,
                       PQfname(result, field_for_rows),
                       false,
                       column_type_alignment(PQftype(result, field_for_rows)));

    // Create reverse mapping for efficient column iteration by rank
    int *horiz_map = pg_malloc(sizeof(int) * num_columns);
    for (int i = 0; i < num_columns; i++) {
        horiz_map[piv_columns[i].rank] = i;
    }

    // Add horizontal headers (data columns)
    char col_align = column_type_alignment(PQftype(result, field_for_data));
    for (int i = 0; i < num_columns; i++) {
        char *colname = piv_columns[horiz_map[i]].name ?
                        piv_columns[horiz_map[i]].name :
                        (popt.nullPrint ? popt.nullPrint : "");
        printTableAddHeader(&cont, colname, false, col_align);
    }
    pg_free(horiz_map);

    // Set up row headers in first column
    for (int i = 0; i < num_rows; i++) {
        int k = piv_rows[i].rank;
        cont.cells[k * (num_columns + 1)] = piv_rows[i].name ?
                                           piv_rows[i].name :
                                           (popt.nullPrint ? popt.nullPrint : "");
    }
    cont.cellsadded = num_rows * (num_columns + 1);

    // Fill in data cells
    for (int rn = 0; rn < PQntuples(result); rn++) {
        pivot_field elt;

        // Find target row
        elt.name = PQgetisnull(result, rn, field_for_rows) ?
                   NULL : PQgetvalue(result, rn, field_for_rows);
        pivot_field *rp = bsearch(&elt, piv_rows, num_rows,
                                 sizeof(pivot_field), pivotFieldCompare);
        int row_number = rp->rank;

        // Find target column
        elt.name = PQgetisnull(result, rn, field_for_columns) ?
                   NULL : PQgetvalue(result, rn, field_for_columns);
        pivot_field *cp = bsearch(&elt, piv_columns, num_columns,
                                 sizeof(pivot_field), pivotFieldCompare);
        int col_number = cp->rank;

        // Place data value in the appropriate cell
        if (col_number >= 0 && row_number >= 0) {
            int idx = 1 + col_number + row_number * (num_columns + 1);

            // Check for duplicate data (error condition)
            if (cont.cells[idx] != NULL) {
                pg_log_error("\\crosstabview: query result contains multiple data values for row \"%s\", column \"%s\"",
                           rp->name ? rp->name : "(null)",
                           cp->name ? cp->name : "(null)");
                goto cleanup;
            }

            cont.cells[idx] = !PQgetisnull(result, rn, field_for_data) ?
                             PQgetvalue(result, rn, field_for_data) :
                             (popt.nullPrint ? popt.nullPrint : "");
        }
    }

    // Fill empty cells with empty strings
    for (int i = 0; i < cont.cellsadded; i++) {
        if (cont.cells[i] == NULL)
            cont.cells[i] = "";
    }

    // Render the table
    printTable(&cont, pset.queryFout, false, pset.logfile);
    success = true;

cleanup:
    printTableCleanup(&cont);
    return success;
}
```