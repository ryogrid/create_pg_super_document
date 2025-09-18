# ArrayBuildStateArr

## Location
src/include/utils/array.h: 205 - 220

## Overview
ArrayBuildStateArr is a working state structure used by accumArrayResultArr() and related functions to efficiently build multi-dimensional arrays by accumulating array inputs rather than scalar elements.

## Definition
```c
typedef struct ArrayBuildStateArr
{
    MemoryContext mcontext;     /* where all the temp stuff is kept */
    char       *data;           /* accumulated data */
    bits8      *nullbitmap;     /* bitmap of is-null flags, or NULL if none */
    int         abytes;         /* allocated length of "data" */
    int         nbytes;         /* number of bytes used so far */
    int         aitems;         /* allocated length of bitmap (in elements) */
    int         nitems;         /* total number of elements in result */
    int         ndims;          /* current dimensions of result */
    int         dims[MAXDIM];   /* dimension bounds array */
    int         lbs[MAXDIM];    /* lower bounds array */
    Oid         array_type;     /* data type of the arrays */
    Oid         element_type;   /* data type of the array elements */
    bool        private_cxt;    /* use private memory context */
} ArrayBuildStateArr;
```

## Detailed Description
ArrayBuildStateArr is designed for building arrays by concatenating existing arrays rather than accumulating individual scalar elements. Unlike ArrayBuildState which works with scalar inputs, this structure handles multi-dimensional array inputs and maintains the dimensional structure of the result. It manages raw data bytes, null bitmaps, and dimension information to construct the final array. The structure is particularly used in aggregate functions that operate on arrays, such as array_agg() when called with array inputs, where the goal is to combine multiple arrays into a larger array structure.

## Parameters / Member Variables
- `mcontext`: Memory context where temporary data and accumulated array data are stored
- `data`: Raw byte buffer containing the accumulated array element data
- `nullbitmap`: Bitmap tracking which elements are NULL, or NULL if no null elements exist
- `abytes`: Currently allocated size in bytes of the data buffer
- `nbytes`: Number of bytes actually used in the data buffer
- `aitems`: Currently allocated capacity of the nullbitmap (in number of elements)
- `nitems`: Total number of elements accumulated in the result array
- `ndims`: Number of dimensions in the result array
- `dims[MAXDIM]`: Array storing the size of each dimension (MAXDIM=6 maximum dimensions)
- `lbs[MAXDIM]`: Array storing the lower bound for each dimension
- `array_type`: OID of the array data type being constructed
- `element_type`: OID of the element data type within the arrays
- `private_cxt`: Flag indicating whether a private memory context is being used

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContext](../M/MemoryContext.md) for memory management
  - bits8 type for null bitmap representation
  - Oid types for array and element type identification
  - MAXDIM constant (defined as 6)
- Called from (representative examples):
  - accumArrayResultArr() - [main](../m/main.md) function for adding array elements
  - initArrayResultArr() - initializes the state for array accumulation
  - makeArrayResultArr() - creates final ArrayType from accumulated array state
  - [array_agg_array_transfn](../a/array_agg_array_transfn.md)() - array aggregation function for array inputs

## Notes and Other Information
- Input must be arrays of the same type, unlike ArrayBuildState which takes scalar elements
- Maximum of 6 dimensions supported (MAXDIM constant)
- The structure efficiently manages memory by growing data and nullbitmap buffers as needed
- Used specifically for array aggregation operations where arrays are concatenated rather than elements collected
- Maintains both dimension bounds and lower bounds to support proper multi-dimensional array semantics
- The returned array type matches the input array type, preserving the array structure