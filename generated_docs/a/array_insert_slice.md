# array_insert_slice

## Location
[src/backend/utils/adt/arrayfuncs.c:5158-5280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5158-L5280)

## Overview
Inserts a slice from a source array into a destination array by replacing elements within a specified slice volume and copying elements outside that volume from the original array.

## Definition

```c
static void
array_insert_slice(ArrayType *destArray,
				   ArrayType *origArray,
				   ArrayType *srcArray,
				   int ndim,
				   int *dim,
				   int *lb,
				   int *st,
				   int *endp,
				   int typlen,
				   bool typbyval,
				   char typalign)
```
## Detailed Description
This static function performs a slice insertion operation on PostgreSQL arrays. It constructs a new array by copying most elements from an original array, but replacing elements within a specified slice volume with consecutive elements from a source array. The function handles multi-dimensional arrays and properly manages null bitmaps for arrays that can contain NULL values.

The operation works by:
1. Copying elements before the slice start from the original array
2. Iterating through the slice volume and replacing each position with elements from the source array
3. Copying any remaining elements after the slice from the original array
4. Properly handling null bitmaps throughout the process

## Parameters / Member Variables
- `*destArray`: The destination array where the result will be stored (must be pre-allocated)
- `*origArray`: The original array from which most elements will be copied
- `*srcArray`: The source array providing replacement elements for the slice volume
- `ndim`: Number of dimensions in the arrays
- `*dim`: Array of dimension sizes for each axis
- `*lb`: Array of lower bounds for each dimension
- `*st`: Array of start indices for the slice in each dimension
- `*endp`: Array of end indices for the slice in each dimension
- `typlen`: Length of the array element type (-1 for variable length)
- `typbyval`: Whether the element type is passed by value
- `typalign`: Alignment requirement for the element type
## Dependencies
- Functions called/Symbols referenced:
  - ARR_DATA_PTR (macro for accessing array data)
  - ARR_NULLBITMAP (macro for accessing null bitmap)
  - [ArrayGetNItems](../A/ArrayGetNItems.md) (calculates total number of items)
  - [ArrayGetOffset](../A/ArrayGetOffset.md) (calculates linear offset from indices)
  - [array_copy](array_copy.md) (copies array elements)
  - [array_bitmap_copy](array_bitmap_copy.md) (copies null bitmap portions)
  - [array_seek](array_seek.md) (advances pointer through array)
  - [mda_get_prod](../m/mda_get_prod.md) (calculates dimension products)
  - [mda_get_range](../m/mda_get_range.md) (calculates slice ranges)
  - [mda_get_offset_values](../m/mda_get_offset_values.md) (calculates offset values)
  - [mda_next_tuple](../m/mda_next_tuple.md) (iterates through multi-dimensional coordinates)
- Called from (representative examples):
  - [array_set_slice](array_set_slice.md) (performs array slice assignment operations)

## Notes and Other Information
- This is a static function, only accessible within arrayfuncs.c
- The caller must verify that slice coordinates are valid before calling this function
- The destination array must be pre-allocated with proper header initialization
- Properly handles both fixed-length and variable-length element types
- Manages null bitmaps correctly for arrays that can contain NULL values
- Uses multi-dimensional array helper functions for coordinate calculations

## Simplified Source

```c
static void
array_insert_slice(ArrayType *destArray,
                   ArrayType *origArray,
                   ArrayType *srcArray,
                   int ndim, int *dim, int *lb,
                   int *st, int *endp,
                   int typlen, bool typbyval, char typalign)
{
    char *destPtr = ARR_DATA_PTR(destArray);
    char *origPtr = ARR_DATA_PTR(origArray);
    char *srcPtr = ARR_DATA_PTR(srcArray);
    bits8 *destBitmap = ARR_NULLBITMAP(destArray);
    bits8 *origBitmap = ARR_NULLBITMAP(origArray);
    bits8 *srcBitmap = ARR_NULLBITMAP(srcArray);
    int orignitems = ArrayGetNItems(ARR_NDIM(origArray), ARR_DIMS(origArray));
    int dest_offset, orig_offset, src_offset;
    int prod[MAXDIM], span[MAXDIM], dist[MAXDIM], indx[MAXDIM];

    // Copy elements before the slice start from original array
    dest_offset = ArrayGetOffset(ndim, dim, lb, st);
    int inc = array_copy(destPtr, dest_offset, origPtr, 0, origBitmap,
                        typlen, typbyval, typalign);
    destPtr += inc;
    origPtr += inc;
    if (destBitmap)
        array_bitmap_copy(destBitmap, 0, origBitmap, 0, dest_offset);

    // Setup multidimensional iteration
    orig_offset = dest_offset;
    mda_get_prod(ndim, dim, prod);
    mda_get_range(ndim, span, st, endp);
    mda_get_offset_values(ndim, dist, prod, span);
    for (int i = 0; i < ndim; i++)
        indx[i] = 0;

    // Process the slice replacement
    src_offset = 0;
    int j = ndim - 1;
    do {
        // Copy elements between previous and current slice position
        if (dist[j]) {
            inc = array_copy(destPtr, dist[j], origPtr, orig_offset,
                           origBitmap, typlen, typbyval, typalign);
            destPtr += inc;
            origPtr += inc;
            if (destBitmap)
                array_bitmap_copy(destBitmap, dest_offset,
                                origBitmap, orig_offset, dist[j]);
            dest_offset += dist[j];
            orig_offset += dist[j];
        }

        // Insert new element from source array at slice position
        inc = array_copy(destPtr, 1, srcPtr, src_offset, srcBitmap,
                        typlen, typbyval, typalign);
        if (destBitmap)
            array_bitmap_copy(destBitmap, dest_offset,
                            srcBitmap, src_offset, 1);
        destPtr += inc;
        srcPtr += inc;
        dest_offset++;
        src_offset++;

        // Skip over original element at this position
        origPtr = array_seek(origPtr, orig_offset, origBitmap, 1,
                           typlen, typbyval, typalign);
        orig_offset++;
    } while ((j = mda_next_tuple(ndim, indx, span)) != -1);

    // Copy any remaining elements after the slice
    array_copy(destPtr, orignitems - orig_offset, origPtr, orig_offset,
              origBitmap, typlen, typbyval, typalign);
    if (destBitmap)
        array_bitmap_copy(destBitmap, dest_offset, origBitmap, orig_offset,
                        orignitems - orig_offset);
}
```