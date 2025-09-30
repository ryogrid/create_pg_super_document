# construct_array

## Location
[src/backend/utils/adt/arrayfuncs.c:3361-3380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3361-L3380)

## Overview
Creates a simple 1-dimensional array object from an array of Datum elements, providing a convenient wrapper around the more complex multi-dimensional array construction function.

## Definition

```c
ArrayType *
construct_array(Datum *elems, int nelems,
				Oid elmtype,
				int elmlen, bool elmbyval, char elmalign)
```
## Detailed Description
The construct_array function provides a simple interface for creating 1-dimensional PostgreSQL array objects. It serves as a convenience wrapper around construct_md_array, automatically setting up the dimensions and lower bounds for a single-dimensional array. The function allocates memory for a new ArrayType structure and copies the provided element values into it, even for pass-by-reference data types. If nelems is 0, the result will be a 0-dimensional array rather than a 1-dimensional empty array.

The function assumes that NULL element values are not supported - all elements must be valid Datum values. Element values are always copied into the array object regardless of whether the data type is pass-by-value or pass-by-reference.

## Parameters / Member Variables
- : Array of Datum items that will become the contents of the constructed array (NULL values not supported)
- : Number of items in the elems array
- : OID of the data type for the array elements
- : Length of the element data type (-1 for variable-length types)
- : Boolean indicating whether elements are passed by value (true) or by reference (false)
- : Alignment requirement for the element data type ('c', 's', 'i', or 'd')

## Dependencies
- Functions called/Symbols referenced:
  - [construct_md_array](construct_md_array.md)
- Called from (representative examples):
  - [StoreAttrMissingVal](../S/StoreAttrMissingVal.md)
  - [StoreAttrDefault](../S/StoreAttrDefault.md)
  - [update_attstats](../u/update_attstats.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)
  - [serialize_expr_stats](../s/serialize_expr_stats.md)
  - [construct_array_builtin](construct_array_builtin.md)
  - [enum_range_internal](../e/enum_range_internal.md)
  - [float8_combine](../f/float8_combine.md)
  - [float8_accum](../f/float8_accum.md)
  - [float4_accum](../f/float4_accum.md)
  - [float8_regr_accum](../f/float8_regr_accum.md)
  - [float8_regr_combine](../f/float8_regr_combine.md)

## Notes and Other Information
- The caller is responsible for providing correct elmlen/elmbyval/elmalign information for performance reasons, rather than having the function look up this information from system catalogs
- The function creates arrays with lower bound of 1 (standard PostgreSQL convention)
- Memory for the array is allocated using palloc and must be freed appropriately by the caller
- This is the most commonly used array construction function for simple 1D arrays in PostgreSQL internals

## Simplified Source

```c
ArrayType *construct_array(Datum *elems, int nelems,
                          Oid elmtype,
                          int elmlen, bool elmbyval, char elmalign) {
    int dims[1];
    int lbs[1];

    // Set up 1D array dimensions
    dims[0] = nelems;    // Number of elements
    lbs[0] = 1;          // Lower bound starts at 1

    // Delegate to multi-dimensional array constructor
    return construct_md_array(elems, NULL, 1, dims, lbs,
                            elmtype, elmlen, elmbyval, elmalign);
}
```