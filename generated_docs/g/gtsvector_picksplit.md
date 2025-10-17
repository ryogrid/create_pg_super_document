# gtsvector_picksplit

## Location
[src/backend/utils/adt/tsgistidx.c:621-802](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L621-L802)

## Overview
Implements the picksplit algorithm for GiST (Generalized Search Tree) indexes on tsvector data, determining how to split a full index page into two balanced pages for optimal search performance.

## Definition

```c
union_l,
				union_r;
```
## Detailed Description
This function is the core of GiST page splitting for text search vector indexes. It analyzes a collection of tsvector signatures and determines the optimal way to partition them into two groups (left and right pages) to maintain balanced tree structure and efficient search operations.

The algorithm works by:
1. Building a cache of signature information for all entries
2. Finding two seed entries that are maximally distant (using Hamming distance)
3. Creating initial left and right partitions based on these seeds
4. Sorting remaining entries by their cost difference between joining left vs right
5. Assigning each entry to the partition that minimizes expansion of the union signature

The function handles both regular signatures and "all true" signatures (where all bits are set), optimizing storage and search efficiency.

## Parameters / Member Variables
- : GistEntryVector containing all entries to be split
- : GIST_SPLITVEC structure to populate with split results
- Returns: Pointer to the populated split vector

## Dependencies
- Functions called/Symbols referenced:
  - : Caches signature information for entries
  - : Calculates Hamming distance between cached signatures  
  - : Calculates Hamming distance between raw signatures
  - : Allocates new tsvector signature structure
  - : Counts set bits in a signature
  - : Comparison function for qsort
  - : Macro to extract entry from vector
  - : Macro to get signature from tsvector
  - : Macro to check if signature has all bits set
- Called from (representative examples):
  - GiST index management during page splits (via function pointer in opclass)

## Notes and Other Information
- File location: src/backend/utils/adt/tsgistidx.c:621-802
- This is a PostgreSQL extension of the standard GiST framework specifically for text search vectors
- The algorithm uses a penalty-based approach with the WISH_F function to maintain balanced splits
- Handles both compressed (signature-based) and uncompressed tsvector representations
- Critical for maintaining good search performance in GIN/GiST text search indexes

## Simplified Source

```c
Datum gtsvector_picksplit(PG_FUNCTION_ARGS) {
    GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
    GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
    int siglen = GET_SIGLEN();
    OffsetNumber maxoff = entryvec->n - 2;

    // Allocate arrays for split results
    v->spl_left = (OffsetNumber *) palloc((maxoff + 2) * sizeof(OffsetNumber));
    v->spl_right = (OffsetNumber *) palloc((maxoff + 2) * sizeof(OffsetNumber));

    // Cache signature information for all entries
    CACHESIGN *cache = (CACHESIGN *) palloc(sizeof(CACHESIGN) * (maxoff + 2));
    char *cache_sign = palloc(siglen * (maxoff + 2));

    for (int j = 0; j < maxoff + 2; j++)
        cache[j].sign = &cache_sign[siglen * j];

    // Find two seed entries that are maximally distant
    int32 waste = -1;
    OffsetNumber seed_1 = 0, seed_2 = 0;

    for (OffsetNumber k = FirstOffsetNumber; k < maxoff; k++) {
        for (OffsetNumber j = k + 1; j <= maxoff; j++) {
            if (k == FirstOffsetNumber)
                fillcache(&cache[j], GETENTRY(entryvec, j), siglen);

            int32 size_waste = hemdistcache(&cache[j], &cache[k], siglen);
            if (size_waste > waste) {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
        }
    }

    // Initialize split with seed entries
    if (seed_1 == 0 || seed_2 == 0) {
        seed_1 = 1;
        seed_2 = 2;
    }

    SignTSVector *datum_l = gtsvector_alloc(SIGNKEY | (cache[seed_1].allistrue ? ALLISTRUE : 0),
                                            siglen, cache[seed_1].sign);
    SignTSVector *datum_r = gtsvector_alloc(SIGNKEY | (cache[seed_2].allistrue ? ALLISTRUE : 0),
                                            siglen, cache[seed_2].sign);

    // Sort remaining entries by cost difference
    SPLITCOST *costvector = (SPLITCOST *) palloc(sizeof(SPLITCOST) * maxoff);
    for (OffsetNumber j = FirstOffsetNumber; j <= maxoff; j++) {
        costvector[j - 1].pos = j;
        int32 size_alpha = hemdistcache(&cache[seed_1], &cache[j], siglen);
        int32 size_beta = hemdistcache(&cache[seed_2], &cache[j], siglen);
        costvector[j - 1].cost = abs(size_alpha - size_beta);
    }
    qsort(costvector, maxoff, sizeof(SPLITCOST), comparecost);

    // Assign entries to left or right based on minimum expansion cost
    v->spl_nleft = v->spl_nright = 0;
    OffsetNumber *left = v->spl_left;
    OffsetNumber *right = v->spl_right;

    for (int k = 0; k < maxoff; k++) {
        OffsetNumber j = costvector[k].pos;

        if (j == seed_1) {
            *left++ = j;
            v->spl_nleft++;
            continue;
        } else if (j == seed_2) {
            *right++ = j;
            v->spl_nright++;
            continue;
        }

        // Calculate cost of adding to left vs right
        int32 size_alpha, size_beta;

        if (ISALLTRUE(datum_l) || cache[j].allistrue) {
            size_alpha = (ISALLTRUE(datum_l) && cache[j].allistrue) ? 0 :
                         SIGLENBIT(siglen) - sizebitvec(cache[j].allistrue ?
                                                        GETSIGN(datum_l) : cache[j].sign, siglen);
        } else {
            size_alpha = hemdistsign(cache[j].sign, GETSIGN(datum_l), siglen);
        }

        if (ISALLTRUE(datum_r) || cache[j].allistrue) {
            size_beta = (ISALLTRUE(datum_r) && cache[j].allistrue) ? 0 :
                        SIGLENBIT(siglen) - sizebitvec(cache[j].allistrue ?
                                                       GETSIGN(datum_r) : cache[j].sign, siglen);
        } else {
            size_beta = hemdistsign(cache[j].sign, GETSIGN(datum_r), siglen);
        }

        // Choose side with lower expansion cost (with balance factor)
        if (size_alpha < size_beta + WISH_F(v->spl_nleft, v->spl_nright, 0.1)) {
            // Add to left - update union signature
            if (ISALLTRUE(datum_l) || cache[j].allistrue) {
                if (!ISALLTRUE(datum_l))
                    memset(GETSIGN(datum_l), 0xff, siglen);
            } else {
                BITVECP ptr = cache[j].sign;
                BITVECP union_l = GETSIGN(datum_l);
                for (int i = 0; i < siglen; i++)
                    union_l[i] |= ptr[i];
            }
            *left++ = j;
            v->spl_nleft++;
        } else {
            // Add to right - update union signature
            if (ISALLTRUE(datum_r) || cache[j].allistrue) {
                if (!ISALLTRUE(datum_r))
                    memset(GETSIGN(datum_r), 0xff, siglen);
            } else {
                BITVECP ptr = cache[j].sign;
                BITVECP union_r = GETSIGN(datum_r);
                for (int i = 0; i < siglen; i++)
                    union_r[i] |= ptr[i];
            }
            *right++ = j;
            v->spl_nright++;
        }
    }

    // Finalize split results
    *right = *left = FirstOffsetNumber;
    v->spl_ldatum = PointerGetDatum(datum_l);
    v->spl_rdatum = PointerGetDatum(datum_r);

    PG_RETURN_POINTER(v);
}
```