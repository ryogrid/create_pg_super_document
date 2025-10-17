# array_slice_size

## Location
[src/backend/utils/adt/arrayfuncs.c:5025-5084](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5025-L5084)

## Overview
Computes the total memory space needed for a slice of a multidimensional array, handling both fixed-length and variable-length element types with null value support.

## Definition

```c
static int
array_slice_size(char *arraydataptr, bits8 *arraynullsptr,
				 int ndim, int *dim, int *lb,
				 int *st, int *endp,
				 int typlen, bool typbyval, char typalign)
```
## Detailed Description
This function calculates the total byte size required to store a specified slice of a multidimensional array. It uses an optimized path for fixed-length types without nulls, simply multiplying the number of elements by the aligned type size. For variable-length types or arrays with nulls, it performs a complex traversal of the slice coordinates, examining each element individually to account for variable sizes and null values. The function leverages PostgreSQL's multidimensional array utilities (mda_*) to navigate the array structure and properly handle dimensional boundaries.

## Parameters
- : Pointer to the start of the array's data portion
- : Pointer to the array's null bitmap, or NULL if the array has no nulls
- : Number of dimensions in the array
- : Array of dimension sizes
- : Array of lower bounds for each dimension
- : Array of starting coordinates for the slice (inclusive)
- : Array of ending coordinates for the slice (inclusive)
- : Storage length of the array element datatype (-1 for variable-length types)
- : Whether the array element datatype is passed by value
- : Alignment requirement of the array element datatype

## Dependencies
- Functions called/Symbols referenced:
  - : Computes the span of the slice in each dimension
  - : Calculates total number of items in a multidimensional span
  - : Computes aligned size for the datatype
  - : Converts multidimensional coordinates to linear offset
  - : Advances pointer over specified number of array elements
  - : Computes products array for multidimensional indexing
  - : Computes offset increments for efficient traversal
  - : Checks if an array element at given offset is null
  - : Computes the size of a variable-length element
  - : Advances to the next tuple in multidimensional iteration
  - : Type used for null bitmap representation
  - : Maximum number of array dimensions supported
- Called from:
  - : Used to determine memory allocation size for slice extraction
  - : Used to calculate space requirements for slice operations

## Notes and Other Information
- This is a static function internal to arrayfuncs.c
- The function assumes that the caller has already verified that the slice coordinates are valid
- Provides an optimization for fixed-length types without nulls using simple arithmetic
- For complex cases (variable-length or with nulls), performs element-by-element traversal
- Uses sophisticated multidimensional array iteration logic to handle arbitrary-dimensional slices
- The traversal respects array element alignment requirements and variable-length element encoding
- Critical for memory management in array slice operations to ensure proper allocation

## Simplified Source

```c
static int
array_slice_size(char *arraydataptr, bits8 *arraynullsptr,
                 int ndim, int *dim, int *lb,
                 int *st, int *endp,
                 int typlen, bool typbyval, char typalign)
{
    int span[MAXDIM], prod[MAXDIM], dist[MAXDIM], indx[MAXDIM];
    int src_offset, count = 0;
    char *ptr;

    // Get the span (size) of the slice in each dimension
    mda_get_range(ndim, span, st, endp);

    // Fast path for fixed-length types without nulls
    if (typlen > 0 && !arraynullsptr)
        return ArrayGetNItems(ndim, span) * att_align_nominal(typlen, typalign);

    // Complex path: traverse slice elements to calculate total size
    src_offset = ArrayGetOffset(ndim, dim, lb, st);
    ptr = array_seek(arraydataptr, 0, arraynullsptr, src_offset,
                    typlen, typbyval, typalign);

    // Setup navigation arrays for multidimensional iteration
    mda_get_prod(ndim, dim, prod);
    mda_get_offset_values(ndim, dist, prod, span);
    for (int i = 0; i < ndim; i++)
        indx[i] = 0;

    // Iterate through all elements in the slice
    int j = ndim - 1;
    do {
        // Skip to next element position if needed
        if (dist[j]) {
            ptr = array_seek(ptr, src_offset, arraynullsptr, dist[j],
                           typlen, typbyval, typalign);
            src_offset += dist[j];
        }

        // Add size of current element if not null
        if (!array_get_isnull(arraynullsptr, src_offset)) {
            int inc = att_addlength_pointer(0, typlen, ptr);
            inc = att_align_nominal(inc, typalign);
            ptr += inc;
            count += inc;
        }
        src_offset++;
    } while ((j = mda_next_tuple(ndim, indx, span)) != -1);

    return count;
}
```