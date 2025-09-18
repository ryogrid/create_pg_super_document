# GIST_SPLITVEC

## Location
[src/include/access/gist.h:140-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist.h#L140-L151)

## Overview
GIST_SPLITVEC is a structure returned by the PickSplit method in GiST indexes, defining how tuples should be divided between left and right pages during a page split operation.

## Definition
```c
typedef struct GIST_SPLITVEC
{
    OffsetNumber *spl_left;         /* array of entries that go left */
    int          spl_nleft;         /* size of this array */
    Datum        spl_ldatum;        /* Union of keys in spl_left */
    bool         spl_ldatum_exists; /* true, if spl_ldatum already exists. */

    OffsetNumber *spl_right;        /* array of entries that go right */
    int          spl_nright;        /* size of the array */
    Datum        spl_rdatum;        /* Union of keys in spl_right */
    bool         spl_rdatum_exists; /* true, if spl_rdatum already exists. */
} GIST_SPLITVEC;
```

## Detailed Description
GIST_SPLITVEC serves as the communication structure between the GiST core code and the PickSplit method implementations. When a GiST index page becomes full and needs to be split, the PickSplit method determines how to distribute the tuples between two new pages to maintain optimal index performance.

The structure supports both primary splits (where all tuples are being divided) and secondary splits (where some decisions have already been made and only a subset of tuples needs placement). For secondary splits, the existing union keys are provided in spl_ldatum/spl_rdatum with corresponding existence flags set to true.

The PickSplit method is responsible for allocating memory for the spl_left and spl_right arrays using palloc, and filling them with the offset numbers of tuples that should go to each side. The union keys represent the bounding or encompassing key that covers all tuples on each side.

## Parameters / Member Variables
- `spl_left`: OffsetNumber* - Array of offset numbers for tuples assigned to the left page (must be palloc'd by PickSplit)
- `spl_nleft`: int - Number of entries in the spl_left array
- `spl_ldatum`: Datum - Union key that encompasses all tuples going to the left page
- `spl_ldatum_exists`: bool - Indicates if spl_ldatum contains a pre-existing union key (for secondary splits)
- `spl_right`: OffsetNumber* - Array of offset numbers for tuples assigned to the right page (must be palloc'd by PickSplit)  
- `spl_nright`: int - Number of entries in the spl_right array
- `spl_rdatum`: Datum - Union key that encompasses all tuples going to the right page
- `spl_rdatum_exists`: bool - Indicates if spl_rdatum contains a pre-existing union key (for secondary splits)

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber
  - Datum
- Called from (representative examples):
  - [fallbackSplit](../f/fallbackSplit.md)
  - [gist_box_picksplit](../g/gist_box_picksplit.md)
  - [supportSecondarySplit](../s/supportSecondarySplit.md)
  - [genericPickSplit](../g/genericPickSplit.md)
  - [gistUserPicksplit](../g/gistUserPicksplit.md)
  - [range_gist_picksplit](../r/range_gist_picksplit.md)
  - [gtsvector_picksplit](../g/gtsvector_picksplit.md)

## Notes and Other Information
- The PickSplit method must palloc both spl_left and spl_right arrays - memory management is the caller's responsibility
- For secondary splits, if the PickSplit method successfully incorporates existing union keys and clears the existence flags, it indicates successful optimization
- If existence flags remain true after PickSplit returns, the GiST core will merge results and recompute union keys from scratch
- The quality of the split decision significantly impacts index performance, as it affects future search and insertion operations
- Different data types implement their own PickSplit strategies optimized for their specific geometric or ordering properties