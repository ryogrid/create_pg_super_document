# brin_form_placeholder_tuple

## Location
[src/backend/access/brin/brin_tuple.c:388-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_tuple.c#L388-L432)

## Overview
Generates a new on-disk tuple with no data values, marked as placeholder, representing a range with no meaningful data in a BRIN index.

## Definition

```c
BrinTuple *
brin_form_placeholder_tuple(BrinDesc *brdesc, BlockNumber blkno, Size *size)
```
## Detailed Description
This function creates a simplified BRIN tuple that serves as a placeholder for block ranges that contain no meaningful data or require special handling. It is a cut-down version of brin_form_tuple that skips value processing and creates a minimal tuple structure with appropriate flags set. The function allocates space for null bitmaps and sets all attributes to "allnulls" state, indicating that no summarizable data exists for the represented range.

The placeholder tuple includes proper null bitmap initialization and sets multiple status flags (NULLS_MASK, PLACEHOLDER_MASK, and EMPTY_RANGE_MASK) to indicate its special nature. This approach allows BRIN indexes to maintain structural integrity while representing ranges that cannot provide meaningful summary information.

## Parameters / Member Variables
- : BRIN descriptor containing schema information needed for tuple structure
- : Block number this placeholder tuple represents in the BRIN index
- : Output parameter to receive the total size of the created placeholder tuple

## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md), BrinTuple (structure types)
  - SizeOfBrinTuple, BITMAPLEN (size calculation macros)
  - BRIN_NULLS_MASK, BRIN_PLACEHOLDER_MASK, BRIN_EMPTY_RANGE_MASK (flag constants)
  - bits8 (bitmap type)
  - HIGHBIT (bit manipulation constant)
  - Memory management functions (palloc0)
- Called from:
  - [summarize_range](../s/summarize_range.md) (src/backend/access/brin/brin.c:1765)
  - BrinTupleIsEmptyRange (src/include/access/brin_tuple.h:98)

## Notes and Other Information
- This is a specialized, optimized version of brin_form_tuple for placeholder scenarios
- Always includes null bitmaps even though no data values are stored
- Sets all attributes to "allnulls" state in the null bitmap
- Does not set "hasnulls" bits since there are no actual data values
- Essential for maintaining BRIN index consistency when ranges contain no summarizable data
- Works in conjunction with brin_form_tuple to provide complete tuple creation functionality

## Simplified Source

```c
BrinTuple *
brin_form_placeholder_tuple(BrinDesc *brdesc, BlockNumber blkno, Size *size)
{
    // Calculate tuple size: always includes null bitmaps
    Size len = SizeOfBrinTuple;
    len += BITMAPLEN(brdesc->bd_tupdesc->natts * 2);
    len = MAXALIGN(len);
    Size hoff = len;

    // Create placeholder tuple
    BrinTuple *rettuple = palloc0(len);
    rettuple->bt_blkno = blkno;
    rettuple->bt_info = hoff;

    // Set all status flags for placeholder
    rettuple->bt_info |= BRIN_NULLS_MASK | BRIN_PLACEHOLDER_MASK | BRIN_EMPTY_RANGE_MASK;

    // Set up null bitmap: mark all attributes as "allnulls"
    bits8 *bitP = ((bits8 *) ((char *) rettuple + SizeOfBrinTuple)) - 1;
    int bitmask = HIGHBIT;

    for (int keyno = 0; keyno < brdesc->bd_tupdesc->natts; keyno++) {
        if (bitmask != HIGHBIT)
            bitmask <<= 1;
        else {
            bitP += 1;
            *bitP = 0x0;
            bitmask = 1;
        }
        *bitP |= bitmask;  // Set allnulls bit
    }

    // No need to set hasnulls bits (they remain 0)

    *size = len;
    return rettuple;
}
```