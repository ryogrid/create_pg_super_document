# array_bitmap_copy

## Location
[src/backend/utils/adt/arrayfuncs.c:4954-5024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4954-L5024)

## Overview
Copies null-bitmap bits from a source array's null bitmap to a destination array's null bitmap, handling bit-level operations for array null value tracking.

## Definition

```c
void
array_bitmap_copy(bits8 *destbitmap, int destoffset,
				  const bits8 *srcbitmap, int srcoffset,
				  int nitems)
```
## Detailed Description
This function performs bit-level copying of null bitmap information between arrays. It handles the intricate bit manipulation required to copy individual null/non-null flags from arbitrary positions in a source bitmap to arbitrary positions in a destination bitmap. The function operates on a bit-by-bit basis, properly handling byte boundaries and bit alignment. When the source bitmap is NULL, it assumes all source elements are non-NULL and sets corresponding destination bits to 1 (non-NULL). The implementation prioritizes simplicity and correctness over optimization, as noted in the comments that it could be optimized using standard bitblt methods.

## Parameters
- : Pointer to the start of the destination array's null bitmap (must not be NULL)
- : 0-based linear element number of the first destination element
- : Pointer to the start of the source array's null bitmap, or NULL if the source has no nulls
- : 0-based linear element number of the first source element
- : Number of bits to copy (must be >= 0)

## Dependencies
- Functions called/Symbols referenced:
  - : Type used for null bitmap representation
  - : PostgreSQL assertion macro for runtime checks
- Called from (representative examples):
  - : Array expression evaluation in executor
  - : Array concatenation operations
  - : Array aggregation combining
  - : Setting individual array elements
  - : Setting array slices
  - : Extracting array slices
  - : Inserting array slices
  - : Array result accumulation
  - : Array result creation

## Notes and Other Information
- This is a public function (not static) in arrayfuncs.c, available throughout PostgreSQL
- Handles the special case where srcbitmap is NULL by setting all corresponding destination bits to 1 (non-NULL)
- Uses bit manipulation with masks (0x100 = 256) to detect byte boundary crossings
- Only modifies the specified bits in the destination bitmap, preserving other bits
- The function includes an early return for nitems <= 0 to prevent memory access violations
- Implementation uses a KISS (Keep It Simple, Stupid) approach rather than optimized bitblt operations
- Critical for maintaining data integrity in array operations involving NULL values
- The Assert(destbitmap) ensures the destination bitmap is never NULL, as this would be a programming error

## Simplified Source

```c
void
array_bitmap_copy(bits8 *destbitmap, int destoffset,
                  const bits8 *srcbitmap, int srcoffset,
                  int nitems)
{
    int destbitmask, destbitval, srcbitmask, srcbitval;

    Assert(destbitmap);
    if (nitems <= 0)
        return;

    // Set up destination pointers and masks
    destbitmap += destoffset / 8;
    destbitmask = 1 << (destoffset % 8);
    destbitval = *destbitmap;

    if (srcbitmap) {
        // Copy from source bitmap
        srcbitmap += srcoffset / 8;
        srcbitmask = 1 << (srcoffset % 8);
        srcbitval = *srcbitmap;

        while (nitems-- > 0) {
            // Copy bit from source to destination
            if (srcbitval & srcbitmask)
                destbitval |= destbitmask;
            else
                destbitval &= ~destbitmask;

            // Advance destination bit position
            destbitmask <<= 1;
            if (destbitmask == 0x100) {
                *destbitmap++ = destbitval;
                destbitmask = 1;
                if (nitems > 0)
                    destbitval = *destbitmap;
            }

            // Advance source bit position
            srcbitmask <<= 1;
            if (srcbitmask == 0x100) {
                srcbitmap++;
                srcbitmask = 1;
                if (nitems > 0)
                    srcbitval = *srcbitmap;
            }
        }
        if (destbitmask != 1)
            *destbitmap = destbitval;
    } else {
        // Source is all non-NULL, set all destination bits to 1
        while (nitems-- > 0) {
            destbitval |= destbitmask;
            destbitmask <<= 1;
            if (destbitmask == 0x100) {
                *destbitmap++ = destbitval;
                destbitmask = 1;
                if (nitems > 0)
                    destbitval = *destbitmap;
            }
        }
        if (destbitmask != 1)
            *destbitmap = destbitval;
    }
}
```