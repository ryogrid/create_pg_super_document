# PrintResultInCrosstab

## Location
[src/bin/psql/crosstabview.c:104-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L104-L285)

## Overview
Main entry point for the crosstab view functionality in psql that processes query result data to generate horizontal and vertical headers, then renders the data in a cross-tabulated format.

## Definition
```c
bool PrintResultInCrosstab(const PGresult *res)
```

## Detailed Description
PrintResultInCrosstab is the primary function responsible for implementing the \crosstabview command in psql. It takes a PostgreSQL query result and transforms it into a cross-tabulated (pivot table) display format. The function performs comprehensive validation, data processing, and coordination of the crosstab rendering process.

The function operates in four main phases:
1. **Validation and Setup**: Validates the result set has the minimum required columns (3) and processes optional arguments for column selection
2. **Data Collection**: Accumulates distinct values for vertical and horizontal headers using AVL trees to ensure uniqueness and efficient sorting
3. **Array Generation**: Converts the AVL tree data into sorted arrays for easier processing
4. **Rendering**: Calls printCrosstab() to generate the actual formatted output

The function supports up to 4 optional arguments via pset.ctv_args:
- [0]: Vertical header column (defaults to column 0)
- [1]: Horizontal header column (defaults to column 1)  
- [2]: Data column (auto-detected if only 3 columns total)
- [3]: Sort column for horizontal headers (optional)

## Parameters / Member Variables
- `*res`: Pointer to PGresult containing the query result data to be cross-tabulated
## Dependencies
- Functions called/Symbols referenced:
  - [avlInit](../a/avlInit.md), avlFree, avlMergeValue, avlCollectFields
  - [PQresultStatus](PQresultStatus.md), PQnfields, PQntuples, PQgetisnull, PQgetvalue
  - [indexOfColumn](../i/indexOfColumn.md), rankSort, printCrosstab
  - [pg_malloc](../p/pg_malloc.md), pg_free, pg_log_error
- Called from (representative examples):
  - [PrintQueryResult](PrintQueryResult.md) (src/bin/psql/common.c:1022)

## Notes and Other Information
- Enforces a maximum of CROSSTABVIEW_MAX_COLUMNS columns to prevent excessive memory usage
- Uses AVL trees for efficient handling of distinct values and automatic sorting
- Requires at least 3 columns in the result set for proper crosstab operation
- Vertical and horizontal header columns must be different
- Memory management includes proper cleanup of allocated arrays and AVL tree structures
- Returns false on any error condition, true on successful processing

## Simplified Source

```c
bool PrintResultInCrosstab(const PGresult *res) {
    bool success = false;
    avl_tree column_headers, row_headers;
    pivot_field *column_array = NULL;
    pivot_field *row_array = NULL;
    int num_columns = 0, num_rows = 0;
    int row_field, column_field, data_field, sort_field;

    // Initialize AVL trees for collecting distinct values
    avlInit(&row_headers);
    avlInit(&column_headers);

    // Validate result set
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("\\crosstabview: statement did not return a result set");
        goto cleanup;
    }

    if (PQnfields(res) < 3) {
        pg_log_error("\\crosstabview: query must return at least three columns");
        goto cleanup;
    }

    // Determine column assignments from arguments or defaults
    row_field = (pset.ctv_args[0] == NULL) ? 0 : indexOfColumn(pset.ctv_args[0], res);
    column_field = (pset.ctv_args[1] == NULL) ? 1 : indexOfColumn(pset.ctv_args[1], res);

    if (row_field < 0 || column_field < 0 || row_field == column_field) {
        pg_log_error("\\crosstabview: invalid column specifications");
        goto cleanup;
    }

    // Determine data column
    if (pset.ctv_args[2] == NULL) {
        if (PQnfields(res) != 3) {
            pg_log_error("\\crosstabview: data column must be specified when query returns more than three columns");
            goto cleanup;
        }
        // Find the remaining column
        for (int i = 0; i < PQnfields(res); i++) {
            if (i != row_field && i != column_field) {
                data_field = i;
                break;
            }
        }
    } else {
        data_field = indexOfColumn(pset.ctv_args[2], res);
        if (data_field < 0) goto cleanup;
    }

    // Optional sort column for horizontal headers
    sort_field = (pset.ctv_args[3] == NULL) ? -1 : indexOfColumn(pset.ctv_args[3], res);

    // Collect distinct values for headers using AVL trees
    for (int row = 0; row < PQntuples(res); row++) {
        // Collect column header values
        char *col_val = PQgetisnull(res, row, column_field) ? NULL :
                        PQgetvalue(res, row, column_field);
        char *sort_val = (sort_field >= 0 && !PQgetisnull(res, row, sort_field)) ?
                         PQgetvalue(res, row, sort_field) : NULL;

        avlMergeValue(&column_headers, col_val, sort_val);

        if (column_headers.count > CROSSTABVIEW_MAX_COLUMNS) {
            pg_log_error("\\crosstabview: maximum number of columns (%d) exceeded",
                         CROSSTABVIEW_MAX_COLUMNS);
            goto cleanup;
        }

        // Collect row header values
        char *row_val = PQgetisnull(res, row, row_field) ? NULL :
                        PQgetvalue(res, row, row_field);
        avlMergeValue(&row_headers, row_val, NULL);
    }

    // Convert AVL trees to sorted arrays
    num_columns = column_headers.count;
    num_rows = row_headers.count;

    column_array = pg_malloc(sizeof(pivot_field) * num_columns);
    row_array = pg_malloc(sizeof(pivot_field) * num_rows);

    avlCollectFields(&column_headers, column_headers.root, column_array, 0);
    avlCollectFields(&row_headers, row_headers.root, row_array, 0);

    // Apply sorting if sort column specified
    if (sort_field >= 0)
        rankSort(num_columns, column_array);

    // Generate the crosstab output
    success = printCrosstab(res,
                           num_columns, column_array, column_field,
                           num_rows, row_array, row_field,
                           data_field);

cleanup:
    avlFree(&column_headers, column_headers.root);
    avlFree(&row_headers, row_headers.root);
    pg_free(column_array);
    pg_free(row_array);

    return success;
}
```