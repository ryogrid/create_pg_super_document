# array_ref

## Location
[src/backend/utils/adt/arrayfuncs.c:3146-3162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3146-L3162)

## Overview
A backwards compatibility wrapper function for array_get_element that provides element access for detoasted/flattened varlena arrays.

## Definition


## Detailed Description
This function serves as a legacy API wrapper around the more general  function. It maintains backwards compatibility for existing code that expects to work with  pointers directly, rather than the more flexible  interface provided by .

The function is specifically designed for:
- **Detoasted arrays**: Arrays that have been decompressed from TOAST storage
- **Flattened arrays**: Arrays in their standard in-memory representation
- **Legacy code compatibility**: Preserving existing APIs that depend on direct ArrayType pointer access

It essentially converts the ArrayType pointer to a Datum and delegates all actual work to .

## Parameters / Member Variables
- : Pointer to the ArrayType structure containing the array data
- : Number of subscript dimensions provided for element access
- : Array of integer subscript values specifying the target element position
- : Type length for the array type (from pg_type.typlen)
- : Type length for individual array elements
- : Boolean indicating whether array elements are passed by value
- : Alignment requirement for array elements (from pg_type.typalign)
- : Output parameter set to true if the retrieved element is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [array_get_element](array_get_element.md) (delegates all actual work)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts ArrayType* to Datum)
- Called from:
  - [pg_get_functiondef](../p/pg_get_functiondef.md)
  - TransformGUCArray
  - GUCArrayAdd
  - GUCArrayDelete
  - GUCArrayReset

## Notes and Other Information
- This is explicitly a backwards compatibility function, maintained to support existing code
- Only works with detoasted and flattened varlena arrays due to the ArrayType* parameter type
- New code should prefer using  directly for better flexibility
- The function adds minimal overhead as it simply wraps the underlying implementation
- Essential for PostgreSQL's configuration system (GUC arrays) and rule utilities
- Located in 