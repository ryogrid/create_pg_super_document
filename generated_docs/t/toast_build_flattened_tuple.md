# toast_build_flattened_tuple

## Location
src/backend/access/heap/heaptoast.c: 563 - 625

## Overview
Builds a heap tuple from Datum arrays while expanding any external TOAST pointers to create a tuple with no out-of-line references.

## Definition
```c
HeapTuple toast_build_flattened_tuple(TupleDesc tupleDesc, Datum *values, bool *isnull)
```

## Detailed Description
The `toast_build_flattened_tuple` function is essentially a variant of `heap_form_tuple` that ensures the resulting tuple contains no external TOAST references. It processes the input Datum array, identifying any externally stored values and retrieving their full content using `detoast_external_attr` before constructing the final tuple.

This function is particularly useful when constructing tuples that need to be self-contained, such as when building result tuples for certain operations where external TOAST access might not be available or efficient. Unlike other flattening functions, it operates on separate values and isnull arrays rather than an existing tuple.

The function preserves the caller's isnull array unchanged but creates a modified copy of the values array to handle detoasted values. It carefully manages memory by tracking which values need to be freed after tuple construction.

## Parameters / Member Variables
- `tupleDesc`: The tuple descriptor describing the target tuple structure
- `values`: Array of Datum values to be included in the tuple
- `isnull`: Array of null indicators corresponding to the values

## Dependencies
- Functions called/Symbols referenced:
  - detoast_external_attr
  - heap_form_tuple
  - VARATT_IS_EXTERNAL
  - MaxTupleAttributeNumber
- Called from (representative examples):
  - ExecEvalWholeRowVar

## Notes and Other Information
- Similar to `heap_form_tuple` but with automatic expansion of external TOAST references
- Does not decompress inline compressed datums or modify short-header values
- Preserves the caller's isnull array unchanged while modifying a copy of the values array
- Implements careful memory management by tracking and freeing temporary detoasted values
- Useful for creating self-contained tuples without external dependencies
- The question of whether to also decompress inline compressed datums remains unresolved (currently they are left compressed)
- Part of PostgreSQL's TOAST system for handling oversized attribute values