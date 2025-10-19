# fallbackSplit

## Location
[src/backend/access/gist/gistproc.c:216-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L216-L276)

## Overview
A simple fallback splitting algorithm for GiST indexes that divides entries into two roughly equal groups when more sophisticated splitting methods fail.

## Definition
```c
static void fallbackSplit(GistEntryVector *entryvec, GIST_SPLITVEC *v)
```

## Detailed Description
This function implements a trivial splitting strategy for GiST (Generalized Search Tree) indexes when dealing with geometric box data types. It serves as a fallback mechanism when more sophisticated splitting algorithms cannot be applied or fail. The algorithm simply divides the entries into two groups: the first half goes to the left page, and the second half goes to the right page.

For each group, the function computes the union (minimal bounding box) of all entries in that group using the adjustBox helper function. This ensures that each resulting page has a proper bounding box that covers all its entries, maintaining the GiST tree invariants.

## Parameters / Member Variables
- `entryvec`: GistEntryVector containing all entries that need to be split
- `v`: GIST_SPLITVEC structure that will be populated with the split result, including:
  - `spl_left`: Array of offset numbers for entries going to the left page
  - `spl_right`: Array of offset numbers for entries going to the right page
  - `spl_nleft`: Number of entries in the left group
  - `spl_nright`: Number of entries in the right group
  - `spl_ldatum`: Union bounding box for the left group
  - `spl_rdatum`: Union bounding box for the right group

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetBoxP](../D/DatumGetBoxP.md): Extracts BOX pointer from Datum
  - [adjustBox](../a/adjustBox.md): Adjusts bounding box to include another box
  - [BoxPGetDatum](../B/BoxPGetDatum.md): Converts BOX pointer to Datum
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - FirstOffsetNumber: Constant for the first valid offset number
  - OffsetNumberNext: Macro to get the next offset number
- Called from (representative examples):
  - [gist_box_picksplit](../g/gist_box_picksplit.md): Uses this as a fallback splitting method

## Notes and Other Information
- Location: src/backend/access/gist/gistproc.c:216-276
- This is a static function, only accessible within the same source file
- The algorithm is simple but not optimal - it doesn't consider the spatial relationships between boxes
- Used when more sophisticated splitting algorithms (like R-tree quadratic split) are not applicable
- Ensures roughly balanced splits by dividing entries in half
- Maintains GiST invariants by computing proper bounding boxes for both resulting groups

## Simplified Source

```c
static void fallbackSplit(GistEntryVector *entryvec, GIST_SPLITVEC *v)
{
    OffsetNumber i, maxoff;
    BOX *unionL = NULL, *unionR = NULL;

    maxoff = entryvec->n - 1;

    // Allocate arrays for left and right split results
    v->spl_left = palloc((maxoff + 2) * sizeof(OffsetNumber));
    v->spl_right = palloc((maxoff + 2) * sizeof(OffsetNumber));
    v->spl_nleft = v->spl_nright = 0;

    // Split entries in half: first half to left, second half to right
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
    {
        BOX *cur = DatumGetBoxP(entryvec->vector[i].key);

        if (i <= (maxoff - FirstOffsetNumber + 1) / 2)
        {
            // Add to left group
            v->spl_left[v->spl_nleft] = i;
            if (unionL == NULL) {
                unionL = palloc(sizeof(BOX));
                *unionL = *cur;
            } else {
                adjustBox(unionL, cur);  // Expand bounding box
            }
            v->spl_nleft++;
        }
        else
        {
            // Add to right group
            v->spl_right[v->spl_nright] = i;
            if (unionR == NULL) {
                unionR = palloc(sizeof(BOX));
                *unionR = *cur;
            } else {
                adjustBox(unionR, cur);  // Expand bounding box
            }
            v->spl_nright++;
        }
    }

    // Set union bounding boxes for both groups
    v->spl_ldatum = BoxPGetDatum(unionL);
    v->spl_rdatum = BoxPGetDatum(unionR);
}
```