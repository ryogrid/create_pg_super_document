# DatumGetExpandedArray

## Location
[src/backend/utils/adt/array_expanded.c:352-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L352-L371)

## Overview
Retrieves a writable expanded array from an input Datum, creating one if necessary to enable efficient array modifications.

## Definition

```c
ExpandedArrayHeader *
DatumGetExpandedArray(Datum d)
```
## Detailed Description
This function provides a convenient interface for obtaining a writable expanded array representation from a Datum. It serves as a key entry point for array manipulation operations that need to modify array contents efficiently.

The function first checks if the input Datum is already a writable expanded array. If so, it returns the existing expanded array directly without any copying or conversion. This optimization avoids unnecessary work when the array is already in the optimal format.

If the input is not a writable expanded array (it could be a flat array, read-only expanded array, or other format), the function calls expand_array() to create a new writable expanded array in the current memory context.

## Parameters / Member Variables
- : Input Datum that should contain an array (either flat or already expanded)

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED_RW
  - [DatumGetPointer](DatumGetPointer.md)
  - DatumGetEOHP
  - [expand_array](../e/expand_array.md)
  - CurrentMemoryContext
- Called from (representative examples):
  - [statext_expressions_load](../s/statext_expressions_load.md)
  - [array_set_element_expanded](../a/array_set_element_expanded.md)
  - PG_GETARG_EXPANDED_ARRAY (macro)
  - AARR_LBOUND

## Notes and Other Information
- **Important safety note**: When the input is already a writable expanded array, this function returns the same object. Callers must ensure their modifications are safe and won't corrupt the array state
- The function uses CurrentMemoryContext when creating new expanded arrays, so the lifetime of returned arrays is tied to the current context
- This is a public interface function (not static) used throughout PostgreSQL for array operations
- The function handles the complexity of different array representations transparently to the caller
- Used extensively in array manipulation functions where write access to array elements is needed