# CopyArrayEls

## Location
src/backend/utils/adt/arrayfuncs.c: 961 - 1015

## Overview
Copies data into an array object from a temporary array of Datums, handling null values and memory management for array construction.

## Definition


## Detailed Description
CopyArrayEls is a core utility function in PostgreSQL's array handling system that efficiently copies element data from temporary Datum arrays into the final ArrayType structure. The function manages both the data portion and the null bitmap of the array, properly aligning data elements according to their type requirements. It handles memory management by optionally freeing pass-by-reference data after copying, which is crucial for preventing memory leaks during array construction.

The function operates by iterating through all elements, setting appropriate bits in the null bitmap for null values, and using ArrayCastAndSet to properly store non-null values with correct alignment. The bitmap management uses bit manipulation to efficiently pack null indicators into bytes.

## Parameters / Member Variables
- : Target ArrayType object with header fields already initialized
- : Array of Datum values to be copied into the array
- : Array of boolean flags indicating null values (can be NULL if no nulls)
- : Number of Datum elements to be copied
- : Length of the element data type (-1 for variable length)
- : Whether the element type is passed by value or reference
- : Alignment requirement for the element data type
- : Whether to free pass-by-reference data values after copying

## Dependencies
- Functions called/Symbols referenced:
  - ARR_DATA_PTR
  - ARR_NULLBITMAP
  - ArrayCastAndSet
  - bits8
- Called from (representative examples):
  - [EA_flatten_into](../E/EA_flatten_into.md)
  - [array_in](../a/array_in.md)
  - [array_recv](../a/array_recv.md)
  - [array_map](../a/array_map.md)
  - [construct_md_array](../c/construct_md_array.md)
  - [array_replace_internal](../a/array_replace_internal.md)

## Notes and Other Information
The caller must ensure that varlena (variable-length) input data is not toasted before calling this function, as the array space has already been allocated. The function automatically disables the freedata flag for pass-by-value types since there's no dynamically allocated memory to free. The null bitmap is managed efficiently using bit manipulation, packing 8 null indicators per byte.