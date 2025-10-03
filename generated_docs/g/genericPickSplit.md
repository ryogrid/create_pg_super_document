# genericPickSplit

## Location
[src/backend/access/gist/gistsplit.c:344-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L344-L414)

## Overview
A fallback split implementation that evenly distributes tuples when the user-defined picksplit function incorrectly places all keys on one side of the split.

## Definition

```c
union datums for each side
	 */
	evec = palloc(sizeof(GISTENTRY) * entryvec->n + GEVHDRSZ);
```
## Detailed Description
This function serves as a safety mechanism for when user-defined picksplit methods fail by putting all keys on the same side of a split, which would be ineffective for index structure. Rather than failing the operation, genericPickSplit implements a simple but reliable strategy:

1. **Even Distribution**: Divides the input tuples in half, with the first half going to the left side and the second half to the right side of the split.

2. **Union Key Generation**: Creates union datums for both sides by calling the union function on the respective tuple sets, ensuring proper bounding keys for each side.

This trivial approach ensures that the split operation can complete successfully even when the user-defined method has bugs, maintaining index functionality.

## Parameters / Member Variables
- : GiST state information containing operator class methods and collation info
- : Vector containing all the index entries to be split
- : The split vector structure to be populated with the split results
- : The attribute number (column) being processed for union key generation

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - OffsetNumberNext
  - memcpy
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Types referenced:
  - [GISTSTATE](../G/GISTSTATE.md)
  - [GistEntryVector](../G/GistEntryVector.md)
  - [GIST_SPLITVEC](../G/GIST_SPLITVEC.md)
  - [GISTENTRY](../G/GISTENTRY.md)
  - OffsetNumber
- Constants used:
  - FirstOffsetNumber
  - GEVHDRSZ
- Called from:
  - [gistUserPicksplit](gistUserPicksplit.md)

## Notes and Other Information
- This function is only invoked as a last resort when user-defined picksplit methods fail
- The even split strategy ensures balanced tree growth, though it may not be optimal for the specific data type
- Union datums are properly computed for both sides to maintain valid index structure
- Memory allocation is performed for both left and right offset arrays to store the split results
- The function handles the edge case gracefully, preventing index corruption from buggy user-defined methods

## Simplified Source

```c
static void
genericPickSplit(GISTSTATE *giststate, GistEntryVector *entryvec,
                 GIST_SPLITVEC *v, int attno)
{
    OffsetNumber i, maxoff;
    int nbytes;
    GistEntryVector *evec;

    maxoff = entryvec->n - 1;
    nbytes = (maxoff + 2) * sizeof(OffsetNumber);

    // Allocate arrays for left and right split sides
    v->spl_left = (OffsetNumber *) palloc(nbytes);
    v->spl_right = (OffsetNumber *) palloc(nbytes);
    v->spl_nleft = v->spl_nright = 0;

    // Split entries evenly: first half to left, second half to right
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        if (i <= (maxoff - FirstOffsetNumber + 1) / 2) {
            // Add to left side
            v->spl_left[v->spl_nleft] = i;
            v->spl_nleft++;
        } else {
            // Add to right side
            v->spl_right[v->spl_nright] = i;
            v->spl_nright++;
        }
    }

    // Create temporary vector for union computation
    evec = palloc(sizeof(GISTENTRY) * entryvec->n + GEVHDRSZ);

    // Compute union datum for left side
    evec->n = v->spl_nleft;
    memcpy(evec->vector, entryvec->vector + FirstOffsetNumber,
           sizeof(GISTENTRY) * evec->n);
    v->spl_ldatum = FunctionCall2Coll(&giststate->unionFn[attno],
                                     giststate->supportCollation[attno],
                                     PointerGetDatum(evec),
                                     PointerGetDatum(&nbytes));

    // Compute union datum for right side
    evec->n = v->spl_nright;
    memcpy(evec->vector, entryvec->vector + FirstOffsetNumber + v->spl_nleft,
           sizeof(GISTENTRY) * evec->n);
    v->spl_rdatum = FunctionCall2Coll(&giststate->unionFn[attno],
                                     giststate->supportCollation[attno],
                                     PointerGetDatum(evec),
                                     PointerGetDatum(&nbytes));
}
```