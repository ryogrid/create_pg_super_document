# ginarrayextract

## Location
[src/backend/access/gin/ginarrayproc.c:33-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginarrayproc.c#L33-L67)

## Overview
This is a PostgreSQL GIN (Generalized Inverted Index) support function that extracts individual elements from an array for indexing purposes.

## Definition

```c
struct_array(array,
					  ARR_ELEMTYPE(array),
					  elmlen, elmbyval, elmalign,
					  &elems, &nulls, &nelems);
```
## Detailed Description
The  function serves as an extractValue support function for GIN indexes on arrays. It takes an input array and decomposes it into its constituent elements, which can then be individually indexed by the GIN access method. This function is fundamental to how PostgreSQL creates GIN indexes on array columns, as it breaks down complex array structures into indexable components.

The function creates a copy of the input array to ensure data persistence during index operations, then uses PostgreSQL's array deconstruction utilities to extract each element along with null flags. This extraction process is essential for GIN's inverted index structure, where each unique element becomes a key in the index.

## Parameters / Member Variables
- : Input array (PG_GETARG_ARRAYTYPE_P_COPY(0)) - the array to be decomposed for indexing
- : Output parameter (int32*) - returns the number of extracted elements  
- : Output parameter (bool**) - returns array of null flags for each element

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P_COPY (macro for getting array argument)
  - [get_typlenbyvalalign](get_typlenbyvalalign.md) (gets type information for array elements)
  - ARR_ELEMTYPE (macro to get array element type)
  - [deconstruct_array](../d/deconstruct_array.md) (decomposes array into elements and null flags)
- Called from:
  - [ginarrayextract_2args](ginarrayextract_2args.md) (wrapper function for 2-argument version)

## Notes and Other Information
- The function makes a copy of the input array to ensure it remains available during index operations
- Memory management is carefully handled - the function notes that the array should not be freed as element pointers reference it
- This is part of PostgreSQL's GIN operator class infrastructure for array types
- The extracted elements and null flags are used by the GIN index to create the inverted index structure