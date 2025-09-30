# array_set

## Location
[src/backend/utils/adt/arrayfuncs.c:3163-3200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3163-L3200)

## Overview
A backwards compatibility wrapper function for array_set_element that provides element assignment for detoasted/flattened varlena arrays using the legacy ArrayType pointer interface.

## Definition

```c
ArrayType *
array_set(ArrayType *array, int nSubscripts, int *indx,
		  Datum dataValue, bool isNull,
		  int arraytyplen, int elmlen, bool elmbyval, char elmalign)
```
## Detailed Description
This function serves as a legacy API wrapper around the more general  function, maintaining backwards compatibility for code that expects to work with  pointers directly. It bridges the gap between the older direct pointer interface and the newer, more flexible Datum-based interface.

Key characteristics:
- **Legacy compatibility**: Preserves existing APIs that depend on direct ArrayType pointer access
- **Limited scope**: Only works with detoasted/flattened varlena arrays due to the ArrayType* parameter constraint
- **Wrapper implementation**: Simply converts between pointer and Datum representations while delegating actual work

The function converts the input ArrayType pointer to a Datum, calls  to perform the actual assignment, and converts the result back to an ArrayType pointer.

## Parameters / Member Variables
- : Pointer to the ArrayType structure containing the source array
- : Number of subscript dimensions provided for element assignment
- : Array of integer subscript values specifying the target element position
- : The new value to assign to the specified array element
- : Boolean indicating whether the new value is NULL
- : Type length for the array type (from pg_type.typlen)
- : Type length for individual array elements  
- : Boolean indicating whether array elements are passed by value
- : Alignment requirement for array elements (from pg_type.typalign)

## Dependencies
- Functions called/Symbols referenced:
  - [array_set_element](array_set_element.md) (performs the actual element assignment)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts ArrayType* to Datum)
  - DatumGetArrayTypeP (converts result Datum back to ArrayType*)
- Called from:
  - [pg_extension_config_dump](../p/pg_extension_config_dump.md)
  - [GUCArrayAdd](../G/GUCArrayAdd.md)
  - [GUCArrayDelete](../G/GUCArrayDelete.md)
  - [GUCArrayReset](../G/GUCArrayReset.md)

## Notes and Other Information
- This is explicitly a backwards compatibility function maintained to support existing code
- Only works with detoasted and flattened varlena arrays due to the ArrayType* parameter type restriction
- New code should prefer using  directly for greater flexibility and expanded array support
- Returns a new ArrayType rather than modifying the original array in place
- Critical for PostgreSQL's configuration system (GUC arrays) and extension management
- The function adds minimal overhead as it's essentially a type conversion wrapper
- Located in arrayfuncs.c with other array manipulation functions

## Simplified Source

```c
ArrayType *
array_set(ArrayType *array, int nSubscripts, int *indx,
          Datum dataValue, bool isNull,
          int arraytyplen, int elmlen, bool elmbyval, char elmalign) {
    // Backwards compatibility wrapper for array_set_element
    // Convert ArrayType* to Datum, call array_set_element, convert result back
    return DatumGetArrayTypeP(array_set_element(PointerGetDatum(array),
                                               nSubscripts, indx,
                                               dataValue, isNull,
                                               arraytyplen,
                                               elmlen, elmbyval, elmalign));
}
``` 