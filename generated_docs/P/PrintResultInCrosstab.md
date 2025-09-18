# PrintResultInCrosstab

## Location
src/bin/psql/crosstabview.c: 104 - 285

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
- : Pointer to PGresult containing the query result data to be cross-tabulated

## Dependencies
- Functions called/Symbols referenced:
  - [avlInit](../a/avlInit.md), avlFree, avlMergeValue, avlCollectFields
  - [PQresultStatus](PQresultStatus.md), PQnfields, PQntuples, PQgetisnull, PQgetvalue
  - [indexOfColumn](../i/indexOfColumn.md), rankSort, printCrosstab
  - pg_malloc, pg_free, pg_log_error
- Called from (representative examples):
  - [PrintQueryResult](PrintQueryResult.md) (src/bin/psql/common.c:1022)

## Notes and Other Information
- Enforces a maximum of CROSSTABVIEW_MAX_COLUMNS columns to prevent excessive memory usage
- Uses AVL trees for efficient handling of distinct values and automatic sorting
- Requires at least 3 columns in the result set for proper crosstab operation
- Vertical and horizontal header columns must be different
- Memory management includes proper cleanup of allocated arrays and AVL tree structures
- Returns false on any error condition, true on successful processing