# ArrayIteratorData

## Location
[src/backend/utils/adt/arrayfuncs.c:68-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L68-L89)

## Overview
ArrayIteratorData is a private structure that maintains the working state for array iteration operations in PostgreSQL. It provides efficient sequential access to array elements with support for both complete array traversal and array slicing.

## Definition

```c
typedef struct ArrayIteratorData
{
	/* basic info about the array, set up during array_create_iterator() */
	ArrayType  *arr;			/* array we're iterating through */
	bits8	   *nullbitmap;		/* its null bitmap, if any */
	int			nitems;			/* total number of elements in array */
	int16		typlen;			/* element type's length */
	bool		typbyval;		/* element type's byval property */
	char		typalign;		/* element type's align property */

	/* information about the requested slice size */
	int			slice_ndim;		/* slice dimension, or 0 if not slicing */
	int			slice_len;		/* number of elements per slice */
	int		   *slice_dims;		/* slice dims array */
	int		   *slice_lbound;	/* slice lbound array */
	Datum	   *slice_values;	/* workspace of length slice_len */
	bool	   *slice_nulls;	/* workspace of length slice_len */

	/* current position information, updated on each iteration */
	char	   *data_ptr;		/* our current position in the array */
	int			current_item;	/* the item # we're at in the array */
}			ArrayIteratorData;
```
## Detailed Description
ArrayIteratorData is the internal working structure used by PostgreSQL's array iteration mechanism. It encapsulates all necessary information for efficiently traversing arrays, including support for both complete array iteration and array slicing operations. The structure is designed to maintain state across multiple function calls during array processing, allowing for memory-efficient streaming of array elements.

The structure is divided into three logical sections: basic array metadata established during initialization, slice-specific configuration for partial array operations, and dynamic position tracking that updates during iteration. This design enables both simple element-by-element iteration and more complex slicing operations while maintaining optimal performance characteristics.

## Parameters / Member Variables
- `*arr`: Pointer to the ArrayType being iterated through
- `*nullbitmap`: Bitmap indicating which array elements are NULL, if any exist
- `nitems`: Total count of elements in the array
- `typlen`: Length of the array's element type (-1 for variable-length types)
- `typbyval`: Boolean indicating if the element type is passed by value
- `typalign`: Alignment requirement for the element type
- `slice_ndim`: Number of dimensions for slicing operation (0 indicates no slicing)
- `slice_len`: Number of elements per slice when slicing is active
- `*slice_dims`: Array of slice dimensions
- `*slice_lbound`: Array of lower bounds for each slice dimension
- `*slice_values`: Workspace array for storing slice element values
- `*slice_nulls`: Workspace array for storing slice element null flags
- `*data_ptr`: Pointer to current position within the array's data
- `current_item`: Zero-based index of current element being processed
## Dependencies
- Functions called/Symbols referenced:
  - bits8 (for null bitmap handling)
  - [ArrayType](ArrayType.md) (array structure definition)
  - Datum (PostgreSQL data value type)

- Called from (representative examples):
  - [array_create_iterator](../a/array_create_iterator.md) (creates and initializes iterator instances)
  - [ArrayIterator](ArrayIterator.md) (typedef pointer to this structure)

## Notes and Other Information
- This structure is declared as private within arrayfuncs.c and is not exposed in header files
- External code accesses this structure through the ArrayIterator typedef, which is a pointer to ArrayIteratorData
- The slice-related members are only populated when performing array slicing operations; they remain unused for simple iteration
- Memory management for workspace arrays (slice_values, slice_nulls) is handled by the array iteration framework
- The structure is designed to support PostgreSQL's set-returning function (SRF) protocol for streaming results