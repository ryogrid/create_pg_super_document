# jsonb_ge

## Location
[src/backend/utils/adt/jsonb_op.c:208-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_op.c#L208-L221)

## Overview
The `jsonb_ge` function implements the "greater than or equal to" comparison operator (>=) for JSONB data types, serving as a B-Tree operator class operator for ordering and range operations.

## Definition
```c
Datum jsonb_ge(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_ge` function performs greater-than-or-equal-to comparison between two JSONB values using the `compareJsonbContainers` function. It returns true if the first JSONB value is lexicographically greater than or equal to the second value (comparison result >= 0). This function is an essential component of the B-Tree operator class for JSONB, enabling efficient range queries, sorting operations, and index scans where inclusive upper bounds are needed.

The function combines both equality and greater-than comparisons into a single operation, making it particularly valuable for range queries where the boundary values should be included in the result set. It maintains consistent ordering behavior with other JSONB comparison functions, ensuring predictable results across different database operations.

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
  - SQL queries using the >= operator with JSONB types
  - Range query processing for inclusive upper bounds
  - B-Tree index operations for boundary checks and range scans

## Notes and Other Information
- Essential for range queries where the upper boundary value should be included in results
- Combines the functionality of both equality and greater-than comparisons (result >= 0)
- Typically invoked through SQL expressions like `jsonb_col >= jsonb_value`
- Critical for implementing BETWEEN clauses and similar inclusive range operations with JSONB data
- Completes the set of comparison operators needed for full B-Tree indexing support on JSONB columns