# spg_quad_leaf_consistent

## Location
[src/backend/access/spgist/spgquadtreeproc.c:407-471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgquadtreeproc.c#L407-L471)

## Overview
An SP-GiST operator function that determines whether a leaf-level tuple satisfies all query constraints in the quadtree spatial index, performing exact matching against scan key conditions.

## Definition

```c
Datum
spg_quad_leaf_consistent(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the final filtering step in PostgreSQL's SP-GiST quadtree index traversal. When the traversal reaches a leaf node, this function:

1. **Performs exact comparisons**: Evaluates the stored point against all query constraints using precise spatial operators
2. **Handles multiple spatial strategies**: Supports various spatial query operations including positional comparisons (left, right, above, below), equality, and containment
3. **Manages distance calculations**: When ORDER BY clauses are present, computes exact distances from the leaf datum to the query points
4. **Returns definitive results**: Unlike inner node consistency checks, leaf checks provide final match/no-match decisions

The function processes each scan key sequentially and returns false immediately if any constraint is not satisfied, providing efficient early termination for non-matching tuples.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (spgLeafConsistentIn*): Input structure with leaf datum, scan keys, and order-by information
  -  (spgLeafConsistentOut*): Output structure populated with match result, leaf value, and distances

## Dependencies
- Functions called/Symbols referenced:
  - [spgLeafConsistentIn](spgLeafConsistentIn.md)/spgLeafConsistentOut
  - [Point](../P/Point.md)
  - [DatumGetPointP](../D/DatumGetPointP.md)
  - SPTEST macro with point comparison functions (point_left, point_right, point_above, point_below, point_eq)
  - [box_contain_pt](../b/box_contain_pt.md)
  - [spg_key_orderbys_distances](spg_key_orderbys_distances.md)
  - Strategy number constants (RTLeftStrategyNumber, RTRightStrategyNumber, etc.)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - SP-GiST framework (via function pointers in operator class)

## Notes and Other Information
- All tests performed are exact comparisons (out->recheck is always set to false)
- The function preserves the original leaf datum as the leaf value in the output
- Supports both point and box query types, with special handling for RTContainedByStrategyNumber
- Uses early termination optimization: stops processing scan keys as soon as one fails
- Distance calculations are only performed for tuples that satisfy all constraints
- The function assumes that for box containment queries, the query argument can be safely cast from Point* to BOX*
- Part of PostgreSQL's extensible indexing framework, specifically designed for 2D spatial data
- Provides the final filtering stage in SP-GiST quadtree queries, ensuring only truly matching tuples are returned

## Simplified Source

```c
Datum
spg_quad_leaf_consistent(PG_FUNCTION_ARGS)
{
    spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
    spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);
    Point *datum = DatumGetPointP(in->leafDatum);
    bool result = true;

    // All tests are exact (no rechecking needed)
    out->recheck = false;
    out->leafValue = in->leafDatum;

    // Check each scan key constraint
    for (int i = 0; i < in->nkeys; i++) {
        Point *query = DatumGetPointP(in->scankeys[i].sk_argument);

        switch (in->scankeys[i].sk_strategy) {
            case RTLeftStrategyNumber:
                result = SPTEST(point_left, datum, query);
                break;
            case RTRightStrategyNumber:
                result = SPTEST(point_right, datum, query);
                break;
            case RTSameStrategyNumber:
                result = SPTEST(point_eq, datum, query);
                break;
            case RTBelowStrategyNumber:
            case RTOldBelowStrategyNumber:
                result = SPTEST(point_below, datum, query);
                break;
            case RTAboveStrategyNumber:
            case RTOldAboveStrategyNumber:
                result = SPTEST(point_above, datum, query);
                break;
            case RTContainedByStrategyNumber:
                // Query is a box, check if point is contained
                result = SPTEST(box_contain_pt, query, datum);
                break;
            default:
                elog(ERROR, "unrecognized strategy number: %d",
                     in->scankeys[i].sk_strategy);
                break;
        }

        // Early termination if any constraint fails
        if (!result)
            break;
    }

    // Calculate distances for ORDER BY if all constraints satisfied
    if (result && in->norderbys > 0)
        out->distances = spg_key_orderbys_distances(in->leafDatum, true,
                                                    in->orderbys, in->norderbys);

    PG_RETURN_BOOL(result);
}
```