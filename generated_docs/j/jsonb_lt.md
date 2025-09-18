# jsonb_lt

## Location
src/backend/utils/adt/jsonb_op.c: 166 - 179

## Overview
The `jsonb_lt` function implements the "less than" comparison operator (<) for JSONB data types, serving as a B-Tree operator class operator for ordering JSONB values.

## Definition
```c
Datum jsonb_lt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_lt` function performs less-than comparison between two JSONB values using the `compareJsonbContainers` function. It returns true if the first JSONB value is lexicographically less than the second value (comparison result < 0). This function is specifically designed as a B-Tree operator class operator, making it essential for indexing, sorting, and ordering operations on JSONB columns in PostgreSQL.

The comparison follows a deterministic ordering scheme that allows JSONB values to be consistently sorted, enabling efficient B-Tree index operations. The function handles memory management properly by freeing any detoasted JSONB structures.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): Left-hand JSONB value in the comparison
  - Second argument (index 1): Right-hand JSONB value in the comparison

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Macro to extract JSONB argument from function call
  - [compareJsonbContainers](../c/compareJsonbContainers.md): Core function that performs deep comparison of JSONB containers
  - `PG_FREE_IF_COPY`: Macro to free copied arguments if necessary
  - `PG_RETURN_BOOL`: Macro to return boolean result as Datum
- Called from (representative examples):
  - SQL queries using the < operator with JSONB types
  - B-Tree index operations for sorting JSONB values
  - ORDER BY clauses involving JSONB columns

## Notes and Other Information
- This function is crucial for B-Tree operator class support, enabling indexing on JSONB columns
- The comparison establishes a total ordering over JSONB values, necessary for consistent sorting
- Typically invoked through SQL expressions like `jsonb_col1 < jsonb_col2`
- The underlying `compareJsonbContainers` function defines the specific ordering rules for different JSONB value types
- Essential for range queries and efficient sorting of JSONB data