# spg_range_quad_inner_consistent

## Location
[src/backend/utils/adt/rangetypes_spgist.c:300-784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_spgist.c#L300-L784)

## Overview
SP-GiST inner node consistent function for range types that determines which child nodes should be visited during index traversal based on query conditions.

## Definition

```c
Datum
spg_range_quad_inner_consistent(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the inner node consistent logic for SP-GiST (Space-Partitioned Generalized Search Tree) indexing of PostgreSQL range types. It analyzes query conditions against the current inner node's centroid to determine which child quadrants need to be visited during index traversal.

The function handles two main cases:
1. **Non-centroid nodes**: Inner nodes without a centroid have exactly 2 child nodes - one for empty ranges and one for non-empty ranges
2. **Centroid nodes**: Inner nodes with a centroid partition the space into 4 or 5 quadrants based on the relationship between ranges and the centroid

For centroid nodes, the quadrants represent:
- Quadrant 1: Ranges with lower bound ≥ centroid upper bound, upper bound ≥ centroid upper bound  
- Quadrant 2: Ranges with lower bound ≤ centroid upper bound, upper bound ≥ centroid upper bound
- Quadrant 3: Ranges with lower bound ≤ centroid upper bound, upper bound ≤ centroid upper bound
- Quadrant 4: Ranges with lower bound ≥ centroid lower bound, upper bound ≤ centroid upper bound
- Quadrant 5: Empty ranges (if present)

The function processes each scan key strategy (BEFORE, OVERLEFT, OVERLAPS, OVERRIGHT, AFTER, ADJACENT, CONTAINS, CONTAINED_BY, EQ, CONTAINS_ELEM) to determine which quadrants could possibly contain matching ranges.

## Parameters / Member Variables
- : Input structure containing scan keys, node information, centroid data, and traversal context
- : Output structure where selected child node numbers and traversal values are stored

## Dependencies
- Functions called/Symbols referenced:
  - RangeIsEmpty
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)  
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - [adjacent_inner_consistent](../a/adjacent_inner_consistent.md)
  - [getQuadrant](../g/getQuadrant.md)
  - [range_cmp_bounds](../r/range_cmp_bounds.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in SP-GiST operator class)

## Notes and Other Information
- For ADJACENT strategy, the function uses  to improve precision by considering previous centroid information
- The function sets  for adjacent searches to pass previous centroid data to child nodes
- Memory for traversal values is allocated in the traversal memory context to persist across index operations
- The 'which' bitmask tracks which child nodes should be visited, with bit N corresponding to child node N-1
- Special handling for  case where all tuples have identical centroid values

## Simplified Source

```c
Datum spg_range_quad_inner_consistent(PG_FUNCTION_ARGS)
{
    spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
    spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);

    bool needPrevious = false;
    int which;

    // Handle special case: all ranges are identical
    if (in->allTheSame) {
        out->nNodes = in->nNodes;
        out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
        for (int i = 0; i < in->nNodes; i++)
            out->nodeNumbers[i] = i;
        PG_RETURN_VOID();
    }

    // Case 1: Node without centroid (empty vs non-empty ranges)
    if (!in->hasPrefix) {
        which = (1 << 1) | (1 << 2);  // Initially visit both nodes

        // Check each scan key against empty/non-empty distinction
        for (int i = 0; i < in->nkeys; i++) {
            StrategyNumber strategy = in->scankeys[i].sk_strategy;
            bool empty = (strategy != RANGESTRAT_CONTAINS_ELEM) ?
                         RangeIsEmpty(DatumGetRangeTypeP(in->scankeys[i].sk_argument)) : false;

            switch (strategy) {
                case RANGESTRAT_BEFORE:
                case RANGESTRAT_OVERLEFT:
                case RANGESTRAT_OVERLAPS:
                case RANGESTRAT_OVERRIGHT:
                case RANGESTRAT_AFTER:
                case RANGESTRAT_ADJACENT:
                    // These strategies return false for empty ranges
                    which = empty ? 0 : (1 << 2);
                    break;
                case RANGESTRAT_CONTAINS:
                    // All ranges contain empty; only non-empty contain non-empty
                    if (!empty) which &= (1 << 2);
                    break;
                case RANGESTRAT_CONTAINED_BY:
                    // Only empty is contained by empty
                    if (empty) which &= (1 << 1);
                    break;
                case RANGESTRAT_CONTAINS_ELEM:
                    which &= (1 << 2);
                    break;
                case RANGESTRAT_EQ:
                    which &= empty ? (1 << 1) : (1 << 2);
                    break;
            }
            if (which == 0) break;
        }
    }
    // Case 2: Node with centroid (quadrant-based partitioning)
    else {
        RangeType *centroid = DatumGetRangeTypeP(in->prefixDatum);
        TypeCacheEntry *typcache = range_get_typcache(fcinfo, RangeTypeGetOid(centroid));

        RangeBound centroidLower, centroidUpper;
        bool centroidEmpty;
        range_deserialize(typcache, centroid, &centroidLower, &centroidUpper, &centroidEmpty);

        which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5);  // All quadrants initially

        // Process each scan key to eliminate impossible quadrants
        for (int i = 0; i < in->nkeys; i++) {
            StrategyNumber strategy = in->scankeys[i].sk_strategy;
            RangeBound lower, upper;
            bool empty;
            RangeType *range = NULL;

            // Convert CONTAINS_ELEM to CONTAINS with point range
            if (strategy == RANGESTRAT_CONTAINS_ELEM) {
                lower.val = upper.val = in->scankeys[i].sk_argument;
                lower.inclusive = upper.inclusive = true;
                lower.infinite = upper.infinite = false;
                lower.lower = true;
                upper.lower = false;
                empty = false;
                strategy = RANGESTRAT_CONTAINS;
            } else {
                range = DatumGetRangeTypeP(in->scankeys[i].sk_argument);
                range_deserialize(typcache, range, &lower, &upper, &empty);
            }

            // Apply strategy-specific constraints on quadrants
            RangeBound *minLower = NULL, *maxLower = NULL;
            RangeBound *minUpper = NULL, *maxUpper = NULL;
            bool inclusive = true;

            switch (strategy) {
                case RANGESTRAT_BEFORE:
                    maxUpper = &lower;
                    inclusive = false;
                    break;
                case RANGESTRAT_OVERLEFT:
                    maxUpper = &upper;
                    break;
                case RANGESTRAT_OVERLAPS:
                    maxLower = &upper;
                    minUpper = &lower;
                    break;
                case RANGESTRAT_OVERRIGHT:
                    minLower = &lower;
                    break;
                case RANGESTRAT_AFTER:
                    minLower = &upper;
                    inclusive = false;
                    break;
                case RANGESTRAT_ADJACENT:
                    if (!empty) {
                        // Complex adjacency logic using previous centroid
                        needPrevious = true;
                        // Simplified: check both adjacency directions
                        int which1 = adjacent_inner_consistent(typcache, &lower, &centroidUpper, NULL) > 0 ?
                                    (1 << 1) | (1 << 4) : (1 << 2) | (1 << 3);
                        int which2 = adjacent_inner_consistent(typcache, &upper, &centroidLower, NULL) > 0 ?
                                    (1 << 1) | (1 << 2) : (1 << 3) | (1 << 4);
                        which &= which1 | which2;
                    }
                    break;
                case RANGESTRAT_CONTAINS:
                    if (!empty) {
                        which &= (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
                        maxLower = &lower;
                        minUpper = &upper;
                    }
                    break;
                case RANGESTRAT_CONTAINED_BY:
                    if (empty) {
                        which &= (1 << 5);  // Empty node only
                    } else {
                        minLower = &lower;
                        maxUpper = &upper;
                    }
                    break;
                case RANGESTRAT_EQ:
                    which &= (1 << getQuadrant(typcache, centroid, range));
                    break;
            }

            // Apply bounding box constraints to eliminate quadrants
            if (minLower && range_cmp_bounds(typcache, &centroidLower, minLower) <= 0)
                which &= (1 << 1) | (1 << 2) | (1 << 5);
            if (maxLower) {
                int cmp = range_cmp_bounds(typcache, &centroidLower, maxLower);
                if (cmp > 0 || (!inclusive && cmp == 0))
                    which &= (1 << 3) | (1 << 4) | (1 << 5);
            }
            if (minUpper && range_cmp_bounds(typcache, &centroidUpper, minUpper) <= 0)
                which &= (1 << 1) | (1 << 4) | (1 << 5);
            if (maxUpper) {
                int cmp = range_cmp_bounds(typcache, &centroidUpper, maxUpper);
                if (cmp > 0 || (!inclusive && cmp == 0))
                    which &= (1 << 2) | (1 << 3) | (1 << 5);
            }

            if (which == 0) break;
        }
    }

    // Build output list of nodes to visit
    out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
    if (needPrevious)
        out->traversalValues = (void **) palloc(sizeof(void *) * in->nNodes);
    out->nNodes = 0;

    MemoryContext oldCtx = MemoryContextSwitchTo(in->traversalMemoryContext);

    for (int i = 1; i <= in->nNodes; i++) {
        if (which & (1 << i)) {
            if (needPrevious) {
                Datum previousCentroid = datumCopy(in->prefixDatum, false, -1);
                out->traversalValues[out->nNodes] = (void *) previousCentroid;
            }
            out->nodeNumbers[out->nNodes] = i - 1;
            out->nNodes++;
        }
    }

    MemoryContextSwitchTo(oldCtx);
    PG_RETURN_VOID();
}
```