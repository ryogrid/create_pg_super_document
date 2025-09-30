# construct_empty_array

## Location
[src/backend/utils/adt/arrayfuncs.c:3568-3584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3568-L3584)

## Overview
Creates a zero-dimensional empty array object of a specified element type, providing the simplest possible array structure in PostgreSQL.

## Definition

```c
ArrayType *
construct_empty_array(Oid elmtype)
```
## Detailed Description
The construct_empty_array function creates the most basic form of array in PostgreSQL - a zero-dimensional array with no elements. This function is used as a building block for other array operations and as a return value when array operations result in empty collections. Unlike multi-dimensional arrays, this function creates a minimal ArrayType structure with just the basic header information and no dimension arrays, lower bound arrays, or element data.

The resulting array has ndim set to 0, dataoffset set to 0 (indicating no null bitmap), and only contains the element type information. This represents the canonical empty array in PostgreSQL's type system.

## Parameters / Member Variables
- : OID of the data type that the empty array would contain if it had elements

## Dependencies
- Functions called/Symbols referenced:
  - SET_VARSIZE
- Called from (representative examples):
  - [construct_md_array](construct_md_array.md)
  - [construct_empty_expanded_array](construct_empty_expanded_array.md)
  - [pg_identify_object_as_address](../p/pg_identify_object_as_address.md)
  - [pg_event_trigger_dropped_objects](../p/pg_event_trigger_dropped_objects.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - [ExecEvalArrayExpr](../E/ExecEvalArrayExpr.md)
  - [array_shuffle_n](../a/array_shuffle_n.md)
  - [array_in](../a/array_in.md)
  - [array_recv](../a/array_recv.md)
  - [array_get_slice](../a/array_get_slice.md)
  - [array_map](../a/array_map.md)
  - [makeArrayResultArr](../m/makeArrayResultArr.md)
  - [array_fill_internal](../a/array_fill_internal.md)
  - [array_replace_internal](../a/array_replace_internal.md)
  - [array_subscript_assign](../a/array_subscript_assign.md)
  - [percentile_disc_multi_final](../p/percentile_disc_multi_final.md)
  - [text_to_array](../t/text_to_array.md)
  - [plperl_array_to_datum](../p/plperl_array_to_datum.md)
  - [PLySequence_ToArray](../P/PLySequence_ToArray.md)

## Notes and Other Information
- Creates the minimal possible ArrayType structure with just basic header information
- The resulting array has zero dimensions (ndim = 0) and no data payload
- Memory allocation is exactly sizeof(ArrayType) with no additional space for elements
- This function is frequently used as a starting point or fallback for array operations that may result in empty collections
- The empty array maintains type information even though it contains no elements
- Used extensively throughout PostgreSQL's array handling code as the canonical representation of empty arrays

## Simplified Source

```c
ArrayType *construct_empty_array(Oid elmtype) {
    // Allocate memory for the basic ArrayType structure
    ArrayType *result = (ArrayType *) palloc0(sizeof(ArrayType));

    // Set the size and basic array properties
    SET_VARSIZE(result, sizeof(ArrayType));
    result->ndim = 0;           // Zero dimensions (empty)
    result->dataoffset = 0;     // No null bitmap
    result->elemtype = elmtype; // Element type for type safety

    return result;
}
```