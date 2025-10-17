# gtsquery_picksplit

## Location
[src/backend/utils/adt/tsquery_gist.c:167-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_gist.c#L167-L272)

## Overview
A GiST picksplit function for TSQuery indexes that implements the node splitting algorithm by finding optimal seeds and distributing entries based on Hamming distance calculations to maintain index efficiency.

## Definition

```c
Datum
gtsquery_picksplit(PG_FUNCTION_ARGS)
```
## Detailed Description
The gtsquery_picksplit function implements the picksplit method for GiST (Generalized Search Tree) indexes on TSQuery data types. This function is responsible for splitting an overfull index node into two nodes when an insertion would exceed the node's capacity.

The algorithm works in several phases:
1. **Seed Selection**: Iterates through all pairs of entries to find the two entries with maximum Hamming distance (most dissimilar signatures) to serve as seeds for the two new nodes.
2. **Cost Vector Creation**: Calculates the cost of assigning each entry to either seed based on the absolute difference of Hamming distances to each seed.
3. **Sorting**: Uses qsort with comparecost to sort entries by assignment cost, processing entries with clearest preferences first.
4. **Distribution**: Assigns each entry to the closer seed, with a bias factor (WISH_F) to maintain balanced node sizes.
5. **Union Calculation**: Updates the node signatures by performing bitwise OR operations as entries are assigned.

This sophisticated approach ensures that similar TSQuery signatures are grouped together while maintaining reasonably balanced node sizes, which is crucial for query performance.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro for function arguments:
  - Argument 0: GistEntryVector pointer containing all entries to be split
  - Argument 1: GIST_SPLITVEC pointer to store the split result

## Dependencies
- Functions called/Symbols referenced:
  - [GistEntryVector](../G/GistEntryVector.md) (vector of GiST entries)
  - [GIST_SPLITVEC](../G/GIST_SPLITVEC.md) (structure to hold split results)  
  - TSQuerySign (TSQuery signature type)
  - SPLITCOST (structure for split cost calculation)
  - [hemdist](../h/hemdist.md) (Hamming distance calculation between signatures)
  - GETENTRY (macro to extract entry from vector)
  - [comparecost](../c/comparecost.md) (comparison function for sorting)
  - qsort (standard library sort function)
  - WISH_F (macro for balancing bias factor)
  - [TSQuerySignGetDatum](../T/TSQuerySignGetDatum.md) (converts TSQuery signature to Datum)
  - FirstOffsetNumber, OffsetNumberNext (offset number utilities)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
- Called from:
  - No direct references found (likely called through GiST function table)

## Notes and Other Information
- This is a PostgreSQL function following the fmgr interface convention
- Part of the GiST access method implementation for TSQuery data types
- The algorithm balances signature similarity with node size considerations
- Uses a sophisticated cost-based approach rather than simple distance-based assignment
- The WISH_F bias factor helps prevent highly unbalanced splits
- Critical for maintaining good GiST index performance during insertions
- Located in src/backend/utils/adt/tsquery_gist.c:167-272
- Returns a Datum containing the GIST_SPLITVEC structure with split results

## Simplified Source

```c
Datum gtsquery_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
    GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
    OffsetNumber maxoff = entryvec->n - 2;
    TSQuerySign left_signature, right_signature;
    int32 waste = -1, size_waste;
    OffsetNumber seed_1 = 0, seed_2 = 0;
    OffsetNumber *left, *right;
    SPLITCOST *costvector;

    // Allocate arrays for left and right entries
    int32 nbytes = (maxoff + 2) * sizeof(OffsetNumber);
    left = v->spl_left = (OffsetNumber *) palloc(nbytes);
    right = v->spl_right = (OffsetNumber *) palloc(nbytes);
    v->spl_nleft = v->spl_nright = 0;

    // Find seeds: pair with maximum Hamming distance
    for (OffsetNumber k = FirstOffsetNumber; k < maxoff; k = OffsetNumberNext(k)) {
        for (OffsetNumber j = OffsetNumberNext(k); j <= maxoff; j = OffsetNumberNext(j)) {
            size_waste = hemdist(GETENTRY(entryvec, j), GETENTRY(entryvec, k));
            if (size_waste > waste) {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
        }
    }

    // Fallback if no good seeds found
    if (seed_1 == 0 || seed_2 == 0) {
        seed_1 = 1;
        seed_2 = 2;
    }

    left_signature = GETENTRY(entryvec, seed_1);
    right_signature = GETENTRY(entryvec, seed_2);

    // Calculate costs for each entry and sort by assignment preference
    maxoff = OffsetNumberNext(maxoff);
    costvector = (SPLITCOST *) palloc(sizeof(SPLITCOST) * maxoff);
    for (OffsetNumber j = FirstOffsetNumber; j <= maxoff; j = OffsetNumberNext(j)) {
        costvector[j - 1].pos = j;
        int32 alpha_dist = hemdist(GETENTRY(entryvec, seed_1), GETENTRY(entryvec, j));
        int32 beta_dist = hemdist(GETENTRY(entryvec, seed_2), GETENTRY(entryvec, j));
        costvector[j - 1].cost = abs(alpha_dist - beta_dist);
    }
    qsort(costvector, maxoff, sizeof(SPLITCOST), comparecost);

    // Distribute entries to left or right based on distance and balance
    for (OffsetNumber k = 0; k < maxoff; k++) {
        OffsetNumber j = costvector[k].pos;

        if (j == seed_1) {
            *left++ = j;
            v->spl_nleft++;
        } else if (j == seed_2) {
            *right++ = j;
            v->spl_nright++;
        } else {
            int32 alpha_dist = hemdist(left_signature, GETENTRY(entryvec, j));
            int32 beta_dist = hemdist(right_signature, GETENTRY(entryvec, j));

            if (alpha_dist < beta_dist + WISH_F(v->spl_nleft, v->spl_nright, 0.05)) {
                left_signature |= GETENTRY(entryvec, j);
                *left++ = j;
                v->spl_nleft++;
            } else {
                right_signature |= GETENTRY(entryvec, j);
                *right++ = j;
                v->spl_nright++;
            }
        }
    }

    // Finalize split results
    *right = *left = FirstOffsetNumber;
    v->spl_ldatum = TSQuerySignGetDatum(left_signature);
    v->spl_rdatum = TSQuerySignGetDatum(right_signature);

    PG_RETURN_POINTER(v);
}
```