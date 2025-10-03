# CopyArrayEls

## Location
[src/backend/utils/adt/arrayfuncs.c:961-1015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L961-L1015)

## Overview
Copies data into an array object from a temporary array of Datums, handling null values and memory management for array construction.

## Definition

```c
void
CopyArrayEls(ArrayType *array,
			 Datum *values,
			 bool *nulls,
			 int nitems,
			 int typlen,
			 bool typbyval,
			 char typalign,
			 bool freedata)
```
## Detailed Description
CopyArrayEls is a core utility function in PostgreSQL's array handling system that efficiently copies element data from temporary Datum arrays into the final ArrayType structure. The function manages both the data portion and the null bitmap of the array, properly aligning data elements according to their type requirements. It handles memory management by optionally freeing pass-by-reference data after copying, which is crucial for preventing memory leaks during array construction.

The function operates by iterating through all elements, setting appropriate bits in the null bitmap for null values, and using ArrayCastAndSet to properly store non-null values with correct alignment. The bitmap management uses bit manipulation to efficiently pack null indicators into bytes.

## Parameters / Member Variables
- `*array`: Target ArrayType object with header fields already initialized
- `*values`: Array of Datum values to be copied into the array
- `*nulls`: Array of boolean flags indicating null values (can be NULL if no nulls)
- `nitems`: Number of Datum elements to be copied
- `typlen`: Length of the element data type (-1 for variable length)
- `typbyval`: Whether the element type is passed by value or reference
- `typalign`: Alignment requirement for the element data type
- `freedata`: Whether to free pass-by-reference data values after copying
## Dependencies
- Functions called/Symbols referenced:
  - ARR_DATA_PTR
  - ARR_NULLBITMAP
  - [ArrayCastAndSet](../A/ArrayCastAndSet.md)
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

## Simplified Source

```c
void CopyArrayEls(ArrayType *array, Datum *values, bool *nulls, int nitems,
                  int typlen, bool typbyval, char typalign, bool freedata) {
    char *p = ARR_DATA_PTR(array);
    bits8 *bitmap = ARR_NULLBITMAP(array);
    int bitval = 0;
    int bitmask = 1;

    // Don't free pass-by-value data (no memory to free)
    if (typbyval)
        freedata = false;

    // Copy each element
    for (int i = 0; i < nitems; i++) {
        if (nulls && nulls[i]) {
            // Handle null element
            if (!bitmap)
                elog(ERROR, "null array element where not supported");
            // Bitmap bit stays 0 for null
        } else {
            // Handle non-null element
            bitval |= bitmask;
            p += ArrayCastAndSet(values[i], typlen, typbyval, typalign, p);

            // Free reference data if requested
            if (freedata)
                pfree(DatumGetPointer(values[i]));
        }

        // Manage null bitmap bit packing
        if (bitmap) {
            bitmask <<= 1;
            if (bitmask == 0x100) {  // 8 bits processed
                *bitmap++ = bitval;
                bitval = 0;
                bitmask = 1;
            }
        }
    }

    // Store final bitmap byte if partially filled
    if (bitmap && bitmask != 1)
        *bitmap = bitval;
}
```