# has_join_restriction

## Location
[src/backend/optimizer/path/joinrels.c:1185-1240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1185-L1240)

## Overview
Detects whether a relation has join-order restrictions due to outer joins, subqueries, lateral references, or multi-relation PlaceHolderVars.

## Definition

```c
union */
			joinrelids = bms_union(rel->relids, rel2->relids);
```
## Detailed Description
The  function is a lightweight screening function that determines whether a given relation is subject to any join-order constraints that would require careful handling during join enumeration. It serves as a quick check to identify relations that participate in complex join scenarios such as outer joins, IN subqueries, lateral references, or PlaceHolderVar computations that span multiple relations.

The function is designed to be conservative, occasionally returning true when restrictions don't actually exist, which is acceptable for its screening purpose. It performs several checks: lateral references (both outgoing and incoming), PlaceHolderVars that require multiple relations for evaluation, and special join constraints from outer joins or subqueries. This allows the optimizer to efficiently identify relations that need special join-order consideration without expensive clause analysis.

## Parameters / Member Variables
- `relids`: The PlannerInfo structure containing global query planning information including placeholder_list and join_info_list
- `relids`: The RelOptInfo to be examined for join-order restrictions
## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [bms_overlap](../b/bms_overlap.md)
- Called from (representative examples):
  - [join_search_one_level](../j/join_search_one_level.md) (multiple locations)

## Notes and Other Information
- Function is static to joinrels.c and not exposed externally
- Designed as a lightweight screening test that may occasionally give false positives
- Returns true immediately if the relation has any lateral references (lateral_relids or lateral_referencers)
- For PlaceHolderVars, only considers those that require multiple relations (not equal to rel's relids)
- Ignores full joins as they are handled by separate mechanisms
- Skips special joins that are already fully contained within the relation
- Tests for overlap rather than containment for partial special join participation
- More efficient than calling have_join_order_restriction() with all possible partners
- Critical for early identification of constrained relations during join enumeration

## Simplified Source

```c
static bool
has_join_restriction(PlannerInfo *root, RelOptInfo *rel)
{
    ListCell *l;

    // Check for lateral references
    if (rel->lateral_relids != NULL || rel->lateral_referencers != NULL)
        return true;

    // Check PlaceHolderVars that span multiple relations
    foreach(l, root->placeholder_list)
    {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

        if (bms_is_subset(rel->relids, phinfo->ph_eval_at) &&
            !bms_equal(rel->relids, phinfo->ph_eval_at))
            return true;
    }

    // Check special join constraints
    foreach(l, root->join_info_list)
    {
        SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(l);

        // Skip full joins - handled separately
        if (sjinfo->jointype == JOIN_FULL)
            continue;

        // Skip if special join is already contained in this relation
        if (bms_is_subset(sjinfo->min_lefthand, rel->relids) &&
            bms_is_subset(sjinfo->min_righthand, rel->relids))
            continue;

        // Restriction exists if relation overlaps but doesn't contain the special join
        if (bms_overlap(sjinfo->min_lefthand, rel->relids) ||
            bms_overlap(sjinfo->min_righthand, rel->relids))
            return true;
    }

    return false;
}
```