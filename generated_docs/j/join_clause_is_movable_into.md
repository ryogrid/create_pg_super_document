# join_clause_is_movable_into

## Location
[src/backend/optimizer/util/restrictinfo.c:670-687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L670-L687)

## Overview  
Tests whether a join clause is movable and can be evaluated within the current join context, considering the available relations and parameterization context.

## Definition
```c
bool join_clause_is_movable_into(RestrictInfo *rinfo, Relids currentrelids, Relids current_and_outer)
```

## Detailed Description
This function determines whether a join clause can be moved to and evaluated at a specific join location defined by the current relation IDs and their outer parameterization context. Unlike join_clause_is_movable_to which tests general movability to a base relation, this function tests movability into a specific join context with known parameterization.

The function performs three key checks:
1. Evaluability: The clause must only reference variables available from current relations plus outer parameterization relations
2. Relevance: The clause must reference at least one variable from current relations to ensure it's pushed to a unique location
3. Outer join safety: The clause cannot be moved into the outer side of its own outer join

The function assumes that lateral reference checks have been performed upstream by the caller, unlike join_clause_is_movable_to which performs these checks internally.

## Parameters / Member Variables  
- `rinfo`: RestrictInfo structure containing the join clause and its relational metadata
- `currentrelids`: Bitmap set of relation IDs representing the proposed evaluation location
- `current_and_outer`: Union of currentrelids and required_outer relids (parameterization's outer relations)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md) (bitmap subset test)
  - [bms_overlap](../b/bms_overlap.md) (bitmap overlap test)
- Called from (representative examples):
  - [has_indexed_join_quals](../h/has_indexed_join_quals.md)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md)
  - [get_joinrel_parampathinfo](../g/get_joinrel_parampathinfo.md)
  - make_simple_restrictinfo

## Notes and Other Information
The API design requires callers to pre-compute the union of current and outer relation IDs for efficiency when applying the function to multiple clauses. The function will always return false if current_and_outer is NULL, which is relied upon by get_joinrel_parampathinfo. This function works in conjunction with join_clause_is_movable_to to support PostgreSQL's parameterized path generation. Note that returning true indicates the clause could be moved to this join relation, but not necessarily that this is the lowest possible join location.

## Simplified Source

```c
bool
join_clause_is_movable_into(RestrictInfo *rinfo,
                            Relids currentrelids,
                            Relids current_and_outer)
{
    // Check if clause can be evaluated with available context
    if (!bms_is_subset(rinfo->clause_relids, current_and_outer))
        return false;

    // Ensure clause references at least one target relation
    if (!bms_overlap(currentrelids, rinfo->clause_relids))
        return false;

    // Prevent moving clause into its outer-join's outer side
    if (bms_overlap(currentrelids, rinfo->outer_relids))
        return false;

    return true;
}
```