# record_image_ne

## Location
[src/backend/utils/adt/rowtypes.c:1753-1758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1753-L1758)

## Overview
The `record_image_ne` function implements the "not equal" comparison operator for PostgreSQL record types based on byte-level image comparison.

## Definition
```c
Datum record_image_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the PostgreSQL SQL function implementation for the != (not equal) operator when performing byte-oriented comparison of record/composite types. It provides a simple wrapper around `record_image_eq`, negating its boolean result to implement the "not equal" semantic.

The function delegates all the complex comparison logic to `record_image_eq` and simply returns the logical negation of its result. This ensures consistency in comparison behavior while providing the complementary inequality operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing the two records to be compared

## Dependencies
- Functions called/Symbols referenced:
  - [record_image_eq](record_image_eq.md)
  - [DatumGetBool](../D/DatumGetBool.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns true when records are not byte-level identical, false when they are identical
- Provides the logical complement to `record_image_eq`
- Inherits all the byte-oriented comparison semantics from the underlying equality function
- Used for inequality testing in contexts requiring physical representation matching
- Simple wrapper function that delegates to `record_image_eq` for consistency
- Located in src/backend/utils/adt/rowtypes.c at lines 1753-1758

## Simplified Source

```c
Datum record_image_ne(PG_FUNCTION_ARGS) {
    // Simply negate the result of record_image_eq
    return !DatumGetBool(record_image_eq(fcinfo));
}
```