# jsonb_gt

## Location
src/backend/utils/adt/jsonb_op.c: 180 - 193

## Overview
The `jsonb_gt` function implements the "greater than" comparison operator (>) for JSONB data types, serving as a B-Tree operator class operator for ordering JSONB values.

## Definition
```c
Datum jsonb_gt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_gt` function performs greater-than comparison between two JSONB values using the `compareJsonbContainers` function. It returns true if the first JSONB value is lexicographically greater than the second value (comparison result > 0). This function is part of the B-Tree operator class for JSONB, enabling efficient indexing, sorting, and range query operations on JSONB columns.

The function establishes a consistent ordering mechanism that allows JSONB values to be compared in a deterministic way, which is essential for database operations like sorting, indexing, and range scans. Memory management is handled properly through the use of `PG_FREE_IF_COPY` macros.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): Left-hand JSONB value in the comparison
  - Second argument (index 1): Right-hand JSONB value in the comparison

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Macro to extract JSONB argument from function call
  - `compareJsonbContainers`: Core function that performs deep comparison of JSONB containers
  - `PG_FREE_IF_COPY`: Macro to free copied arguments if necessary
  - `PG_RETURN_BOOL`: Macro to return boolean result as Datum
- Called from (representative examples):
  - SQL queries using the > operator with JSONB types
  - B-Tree index operations for sorting JSONB values
  - Range query processing involving JSONB columns

## Notes and Other Information
- Essential component of the B-Tree operator class for JSONB, enabling indexing capabilities
- Provides the complementary operation to `jsonb_lt` for establishing total ordering
- Typically invoked through SQL expressions like `jsonb_col1 > jsonb_col2`
- Works in conjunction with other comparison operators to support complex query predicates
- The deterministic ordering enables consistent results across different query executions