# placeOne

## Location
[src/backend/access/gist/gistsplit.c:200-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L200-L236)

## Overview
Places a single don't-care tuple into either the left or right side of a GiST split based on which side has the least penalty.

## Definition

```c
static void
placeOne(Relation r, GISTSTATE *giststate, GistSplitVector *v,
		 IndexTuple itup, OffsetNumber off, int attno)
```
## Detailed Description
This function determines the optimal placement for a single don't-care tuple by comparing the penalty of adding it to either the left or right side of the split. It evaluates penalties for all columns starting from the specified  and places the tuple on the side with lower penalty. If penalties are equal for all columns, the tuple defaults to the left side. The function works by decompressing the tuple's attributes and computing penalties against the current union keys for both sides of the split.

## Parameters / Member Variables
- : Relation (table/index) being operated on
- : Pointer to GISTSTATE structure containing GiST access method information and operator class functions
- : Pointer to GistSplitVector structure containing split information including:
  - : Array of left-side union keys for each attribute
  - : Array of right-side union keys for each attribute  
  - : Array of null indicators for left-side union keys
  - : Array of null indicators for right-side union keys
  - : Array of left-side tuple indices
  - : Count of left-side tuples
  - : Array of right-side tuple indices
  - : Count of right-side tuples
- : IndexTuple to be placed
- : OffsetNumber of the tuple being placed
- : Starting attribute number for penalty evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](../G/GISTSTATE.md) (structure type)
  - [GistSplitVector](../G/GistSplitVector.md) (structure type)
  - [GISTENTRY](../G/GISTENTRY.md) (structure type)
  - INDEX_MAX_KEYS (constant for maximum index keys)
  - [gistDeCompressAtt](../g/gistDeCompressAtt.md) (decompresses tuple attributes)
  - gistentryinit (initializes GISTENTRY structures)
  - [gistpenalty](../g/gistpenalty.md) (computes penalty for adding tuple to union)
- Called from:
  - [gistUserPicksplit](../g/gistUserPicksplit.md) (at src/backend/access/gist/gistsplit.c:565)

## Notes and Other Information
- This is a static function, only accessible within the gistsplit.c file
- Used exclusively for placing don't-care tuples identified by 
- Defaults to left side placement when penalties are equal across all attributes
- Evaluates penalties starting from a specific attribute number, allowing for column-specific optimization
- Updates the appropriate split vector arrays and counters after placement decision
- Part of the split optimization process that redistributes flexible tuples for better balance
- The penalty computation considers the geometric/spatial relationship between the tuple and existing union keys

## Simplified Source

```c
static void
placeOne(Relation r, GISTSTATE *giststate, GistSplitVector *v,
         IndexTuple itup, OffsetNumber off, int attno)
{
    GISTENTRY identry[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    bool toLeft = true;

    // Decompress tuple attributes
    gistDeCompressAtt(giststate, r, itup, NULL, 0, identry, isnull);

    // Compare penalties for each attribute starting from attno
    for (int i = attno; i < giststate->nonLeafTupdesc->natts; i++) {
        GISTENTRY entry;
        float lpenalty, rpenalty;

        // Calculate penalty for left side
        gistentryinit(entry, v->spl_lattr[i], r, NULL, 0, false);
        lpenalty = gistpenalty(giststate, i, &entry, v->spl_lisnull[i],
                               identry + i, isnull[i]);

        // Calculate penalty for right side
        gistentryinit(entry, v->spl_rattr[i], r, NULL, 0, false);
        rpenalty = gistpenalty(giststate, i, &entry, v->spl_risnull[i],
                               identry + i, isnull[i]);

        // If penalties differ, choose side with lower penalty
        if (lpenalty != rpenalty) {
            if (lpenalty > rpenalty)
                toLeft = false;
            break;
        }
    }

    // Add tuple to chosen side
    if (toLeft)
        v->splitVector.spl_left[v->splitVector.spl_nleft++] = off;
    else
        v->splitVector.spl_right[v->splitVector.spl_nright++] = off;
}
```