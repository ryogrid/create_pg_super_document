# array_iter

## Location
[src/include/utils/arrayaccess.h:33-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/arrayaccess.h#L33-L45)

## Overview
A state structure that maintains iteration context for sequentially accessing elements in PostgreSQL arrays, supporting both expanded and flat array storage formats.

## Definition
```c
typedef struct array_iter
{
    /* datumptr being NULL or not tells if we have flat or expanded array */
    
    /* Fields used when we have an expanded array */
    Datum      *datumptr;        /* Pointer to Datum array */
    bool       *isnullptr;       /* Pointer to isnull array */
    
    /* Fields used when we have a flat array */
    char       *dataptr;         /* Current spot in the data area */
    bits8      *bitmapptr;       /* Current byte of the nulls bitmap, or NULL */
    int         bitmask;         /* mask for current bit in nulls bitmap */
} array_iter;
```

## Detailed Description
The `array_iter` structure serves as an iterator state for efficiently traversing PostgreSQL arrays in sequential order. It acts as a unified interface that can handle two different array storage formats transparently:

1. **Expanded Arrays**: Arrays that have been deconstructed into separate Datum and null flag arrays for faster access. These use the `datumptr` and `isnullptr` fields.

2. **Flat Arrays**: Arrays stored in PostgreSQL's compact binary format where elements are packed together with a null bitmap. These use the `dataptr`, `bitmapptr`, and `bitmask` fields.

The structure is designed so that the presence or absence of `datumptr` determines which set of fields is active. The iterator maintains state between calls to `array_iter_next`, tracking the current position in the data area and the current bit position in the null bitmap for flat arrays.

## Parameters / Member Variables
- `datumptr`: Pointer to an array of Datum values in expanded arrays (NULL for flat arrays)
- `isnullptr`: Pointer to an array of boolean null flags in expanded arrays (NULL for flat arrays or when no nulls exist)
- `dataptr`: Current position in the binary data area when iterating through flat arrays (NULL for expanded arrays)
- `bitmapptr`: Pointer to the current byte in the null bitmap for flat arrays (NULL for expanded arrays or when no null bitmap exists)
- `bitmask`: Bit mask (1-255) indicating which bit in the current bitmap byte represents the current element's null status

## Dependencies
- Functions called/Symbols referenced:
  - bits8 (data type for bitmap bytes)
- Called from (representative examples):
  - [array_out](array_out.md)
  - [array_send](array_send.md)
  - [array_map](array_map.md)
  - [array_eq](array_eq.md)
  - [array_cmp](array_cmp.md)
  - [hash_array](../h/hash_array.md)
  - [hash_array_extended](../h/hash_array_extended.md)
  - [array_contain_compare](array_contain_compare.md)
  - [array_unnest](array_unnest.md)
  - [array_iter_setup](array_iter_setup.md)
  - [array_iter_next](array_iter_next.md)

## Notes and Other Information
- The structure must be initialized using `array_iter_setup()` before use
- Elements are accessed sequentially using `array_iter_next()` with increasing index values
- The `datumptr` field serves as a discriminator: non-NULL means expanded array, NULL means flat array
- For flat arrays, `bitmask` starts at 1 and shifts left for each element, wrapping every 8 elements
- The structure optimizes memory access patterns by maintaining current positions rather than recalculating them
- Supports arrays with or without null values through conditional null bitmap handling
- Defined as a typedef struct in arrayaccess.h for use throughout the array processing subsystem
- The iterator design allows the same calling code to work efficiently with both array storage formats