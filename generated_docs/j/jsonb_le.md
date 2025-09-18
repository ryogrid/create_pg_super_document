# jsonb_le

## Location
src/backend/utils/adt/jsonb_op.c: 194 - 207

## Overview
The `jsonb_le` function implements the "less than or equal to" comparison operator (<=) for JSONB data types, serving as a B-Tree operator class operator for ordering and range operations.

## Definition
```c
Datum jsonb_le(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_le` function performs less-than-or-equal-to comparison between two JSONB values using the `compareJsonbContainers` function. It returns true if the first JSONB value is lexicographically less than or equal to the second value (comparison result <= 0). This function is part of the B-Tree operator class infrastructure, enabling efficient range queries, sorting operations, and index scans on JSONB columns.

The function combines both equality and less-than comparisons into a single operation, making it particularly useful for range queries where the boundary values should be included. It maintains the same deterministic ordering scheme as other JSONB comparison functions, ensuring consistent behavior across different database operations.

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
  - SQL queries using the <= operator with JSONB types
  - Range query processing for inclusive lower bounds
  - B-Tree index operations for boundary checks

## Notes and Other Information
- Critical for range queries where the boundary value should be included in the result set
- Combines the functionality of both equality and less-than comparisons (result <= 0)
- Typically invoked through SQL expressions like `jsonb_col <= jsonb_value`
- Essential for implementing BETWEEN clauses and similar range operations with JSONB data
- Works together with other comparison operators to provide complete ordering support for JSONB indexing