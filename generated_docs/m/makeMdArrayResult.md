# makeMdArrayResult

## Location
[src/backend/utils/adt/arrayfuncs.c:5440-5491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5440-L5491)

## Overview
Produces a multi-dimensional final result from an ArrayBuildState structure, allowing creation of arrays with specified dimensions and bounds.

## Definition

```c
struct_md_array(astate->dvalues,
								astate->dnulls,
								ndims,
								dims,
								lbs,
								astate->element_type,
								astate->typlen,
								astate->typbyval,
								astate->typalign);
```
## Detailed Description
This function creates a multi-dimensional array from the accumulated data in an ArrayBuildState. It constructs the final ArrayType result in the specified result context (rcontext) and optionally cleans up the working state. The function provides flexibility in defining array dimensions and lower bounds, making it suitable for creating complex multi-dimensional arrays.

The function switches to the result memory context to construct the array, then switches back. If release is true and the astate was initialized with its own memory context, it will delete the working memory context to free resources.

## Parameters / Member Variables
- : Working ArrayBuildState containing accumulated values and nulls (must not be NULL)
- : Number of dimensions for the resulting array
- : Array of dimension sizes for each dimension
- : Array of lower bounds for each dimension
- : Memory context where the result array should be constructed
- : Whether it's safe to release/delete the working state memory context

## Dependencies
- Functions called/Symbols referenced:
  - construct_md_array
  - MemoryContextSwitchTo
  - MemoryContextDelete
  - PointerGetDatum
- Called from (representative examples):
  - array_agg_finalfn
  - makeArrayResult
  - makeArrayResultAny
  - populate_array

## Notes and Other Information
- **Warning**: No validation is performed to ensure the specified dimensions match the number of accumulated values
- The release parameter should only be set to true if the astate was initialized with subcontext=true (its own memory context)
- If release=false, the caller is responsible for cleaning up the astate memory context appropriately
- The function is commonly used in aggregate functions and array construction scenarios where multi-dimensional arrays are needed