# array_map

## Location
[src/backend/utils/adt/arrayfuncs.c:3201-3360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3201-L3360)

## Overview
Transforms each element of an array through an arbitrary expression, returning a new array with the same dimensions but potentially different element types.

## Definition


## Detailed Description
This function implements a higher-order array transformation operation, similar to the map function in functional programming languages. It applies a given expression to each element of the source array and constructs a new array with the results.

Key features include:
1. **Element-wise transformation**: Applies the expression to each individual array element
2. **Type transformation**: Can change element types between input and output arrays (if binary-compatible)
3. **Dimension preservation**: Maintains the same array structure (dimensions, bounds) as the source
4. **Null handling**: Properly processes NULL elements and maintains null bitmap when needed
5. **Performance optimization**: Uses ArrayMapState for caching type information across multiple calls
6. **Expression evaluation**: Leverages PostgreSQL's expression evaluation framework for transformations

The function is designed for efficient bulk transformations and integrates with PostgreSQL's expression evaluation system.

## Parameters / Member Variables
- : Datum representing the source array to be transformed
- : Compiled expression state representing the per-element transformation
- : Expression evaluation context providing variable bindings and memory management
- : OID of the element type for the output array (must be binary-compatible with expression result)
- : Workspace for array_map operations that caches type information for performance (must be zeroed before first use)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetAnyArrayP
  - ArrayGetNItems
  - [construct_empty_array](../c/construct_empty_array.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - array_iter_setup
  - array_iter_next
  - ExecEvalExpr
  - PG_DETOAST_DATUM
  - att_addlength_datum
  - att_align_nominal
  - AllocSizeIsValid
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - SET_VARSIZE
- Called from:
  - [ExecEvalArrayCoerce](../E/ExecEvalArrayCoerce.md)

## Notes and Other Information
- The caller must ensure the input array is not NULL (NULL elements within the array are acceptable)
- The caller should run in the econtext's per-tuple memory context for proper memory management
- [ArrayMapState](../A/ArrayMapState.md) can be reused across multiple calls for better performance by caching type lookup information
- Source elements are placed in  and  for expression evaluation
- Handles both fixed-length and variable-length element types with proper alignment and detoasting
- Returns an empty array if the source array is empty
- Includes overflow protection when calculating result array size
- The function does not attempt to free results from expression evaluation to avoid corruption
- Essential for array type coercion operations in PostgreSQL's executor
- Located in 