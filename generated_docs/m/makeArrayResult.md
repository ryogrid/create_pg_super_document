# makeArrayResult

## Location
[src/backend/utils/adt/arrayfuncs.c:5408-5439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5408-L5439)

## Overview
Converts an ArrayBuildState into a final one-dimensional PostgreSQL array, handling both empty and non-empty cases while managing memory context cleanup.

## Definition

```c
Datum
makeArrayResult(ArrayBuildState *astate,
				MemoryContext rcontext)
```
## Detailed Description
This function finalizes the array building process by converting an ArrayBuildState structure into a proper PostgreSQL array Datum. It creates a one-dimensional array with appropriate dimensions and lower bounds, delegating the actual array construction to makeMdArrayResult().

The function handles two cases:
- Non-empty arrays: Creates a 1-D array with nelems elements and lower bound of 1
- Empty arrays: Creates a 0-dimensional empty array (ndims = 0)

Memory management is handled according to the private_cxt flag: if the ArrayBuildState was created with a separate memory context (subcontext=true), that context is cleaned up by makeMdArrayResult().

## Parameters / Member Variables
- : Working state containing accumulated array elements (must not be NULL)
- : Memory context where the final array result should be constructed

## Dependencies
- Functions called/Symbols referenced:
  - [makeMdArrayResult](makeMdArrayResult.md) (performs the actual multi-dimensional array construction)
- Called from (representative examples):
  - [array_positions](../a/array_positions.md) (finding positions of elements in arrays)
  - [parse_ident](../p/parse_ident.md) (identifier parsing functions)
  - [regexp_split_to_array](../r/regexp_split_to_array.md) (regular expression splitting)
  - [text_to_array](../t/text_to_array.md) (text splitting functions)
  - [xpath](../x/xpath.md) (XML path expression functions)

## Notes and Other Information
- Always creates one-dimensional arrays with lower bound of 1
- Properly handles empty arrays by setting ndims to 0
- Memory cleanup is conditional on the private_cxt flag from initialization
- The astate parameter must not be NULL (unlike accumArrayResult)
- Returns a Datum that represents a complete PostgreSQL array
- This is the standard way to finalize array building in the newer scheme
- The result array is allocated in the specified rcontext, not the build context

## Simplified Source

```c
Datum makeArrayResult(ArrayBuildState *astate, MemoryContext rcontext) {
    int ndims, dims[1], lbs[1];

    // Set array dimensions: 1D if elements exist, 0D if empty
    if (astate->nelems > 0) {
        ndims = 1;
        dims[0] = astate->nelems;  // Number of elements
        lbs[0] = 1;               // Lower bound starts at 1
    } else {
        ndims = 0;  // Empty array
    }

    // Delegate to multi-dimensional array builder
    return makeMdArrayResult(astate, ndims, dims, lbs, rcontext,
                             astate->private_cxt);
}
```