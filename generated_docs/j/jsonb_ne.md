# jsonb_ne

## Location
[src/backend/utils/adt/jsonb_op.c:149-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_op.c#L149-L165)

## Overview
The `jsonb_ne` function implements the "not equal" comparison operator (<>) for JSONB data types, returning true if two JSONB values are not equal.

## Definition
```c
Datum jsonb_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_ne` function performs inequality comparison between two JSONB values by leveraging the `compareJsonbContainers` function to compare the root containers of the two input JSONB structures. It returns true if the comparison result is non-zero (indicating the values are different), and false if they are equal. The function follows PostgreSQL's function calling convention using the `PG_FUNCTION_ARGS` macro and returns a `Datum` type.

The function properly handles memory management by using `PG_FREE_IF_COPY` to free any copied JSONB structures that were detoasted during the function call, ensuring no memory leaks occur.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument (index 0): First JSONB value to compare
  - Second argument (index 1): Second JSONB value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_JSONB_P`: Macro to extract JSONB argument from function call
  - [compareJsonbContainers](../c/compareJsonbContainers.md): Core function that performs deep comparison of JSONB containers
  - `PG_FREE_IF_COPY`: Macro to free copied arguments if necessary
  - `PG_RETURN_BOOL`: Macro to return boolean result as Datum
- Called from (representative examples):
  - SQL queries using the <> operator with JSONB types
  - Internal PostgreSQL expression evaluation

## Notes and Other Information
- This function is typically invoked through SQL operator expressions like `jsonb_col1 <> jsonb_col2`
- The actual comparison logic is delegated to `compareJsonbContainers`, making this function a thin wrapper
- Memory management is handled correctly with `PG_FREE_IF_COPY` to prevent memory leaks
- Returns the logical negation of equality comparison (true when values differ)

## Simplified Source

```c
Datum jsonb_ne(PG_FUNCTION_ARGS) {
    Jsonb *jba = PG_GETARG_JSONB_P(0);
    Jsonb *jbb = PG_GETARG_JSONB_P(1);

    // Compare containers and return true if different
    bool res = (compareJsonbContainers(&jba->root, &jbb->root) != 0);

    PG_FREE_IF_COPY(jba, 0);
    PG_FREE_IF_COPY(jbb, 1);
    PG_RETURN_BOOL(res);
}
```