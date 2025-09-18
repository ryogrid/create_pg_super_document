# array_set_slice

## Location
[src/backend/utils/adt/arrayfuncs.c:2806-3145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L2806-L3145)

## Overview
Implements slice assignment operations on PostgreSQL arrays, allowing assignment of values to a range of array positions specified by upper and lower bounds.

## Definition


## Detailed Description
This function performs slice assignment operations on PostgreSQL arrays, replacing a range of elements (defined by upper and lower subscript boundaries) with values from a source array. Key features include:

1. **Flexible slicing**: Supports both full and partial boundary specification - omitted bounds default to array limits
2. **Array extension**: For one-dimensional arrays, allows assignment beyond existing bounds, automatically extending the array and filling gaps with NULLs
3. **Multi-dimensional support**: Handles multi-dimensional arrays but restricts extension to prevent complexity
4. **Memory management**: Creates a new array rather than modifying the original, ensuring immutability
5. **Type safety**: Validates array dimensions, bounds, and source array size compatibility

The function handles both ordinary varlena arrays and provides comprehensive bounds checking with overflow protection.

## Parameters / Member Variables
- : The target array to be modified (must not be NULL)
- : Number of subscript dimensions provided
- : Array of upper boundary subscript values for the slice
- : Array of lower boundary subscript values for the slice  
- : Boolean flags indicating which upper boundaries are explicitly provided
- : Boolean flags indicating which lower boundaries are explicitly provided
- : Source array containing the replacement values
- : Indicates whether the source array is NULL (results in no-op)
- : Type length for the array type (pg_type.typlen)
- : Type length for individual array elements
- : Whether array elements are passed by value
- : Alignment requirement for array elements

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - [deconstruct_array](../d/deconstruct_array.md)
  - [construct_md_array](../c/construct_md_array.md)
  - ArrayGetNItems
  - ArrayCheckBounds
  - [mda_get_range](../m/mda_get_range.md)
  - [array_nelems_size](array_nelems_size.md)
  - [array_slice_size](array_slice_size.md)
  - [array_insert_slice](array_insert_slice.md)
  - [array_bitmap_copy](array_bitmap_copy.md)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
- Called from:
  - [array_subscript_assign_slice](array_subscript_assign_slice.md)

## Notes and Other Information
- Fixed-length arrays are not currently supported for slice operations
- Assignment from NULL source arrays is treated as a no-op
- Multi-dimensional arrays cannot be extended during slice assignment (restriction may be lifted in future)
- Uses overflow-safe arithmetic to prevent integer overflow when calculating new array dimensions
- For one-dimensional arrays, automatically fills gaps between existing elements and new slice boundaries with NULLs
- The function assumes it's safe to modify the provided index arrays (lowerIndx[], upperIndx[])
- Index arrays must be sized to MAXDIM even when fewer subscripts are used
- Source array size validation ensures sufficient elements are available for the slice assignment
- Located in 