# join_is_removable

## Location
[src/backend/optimizer/plan/analyzejoins.c:160-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L160-L328)

## Overview
Determines whether a special join (specifically a left join) can be eliminated from the query plan because it will not affect the result set.

## Definition

```c
union(sjinfo->min_lefthand, sjinfo->min_righthand);
```
## Detailed Description
This function implements a critical optimization in PostgreSQL's query planner by analyzing whether a left join can be safely removed without changing the query semantics. The optimization is based on the principle that a left join can be eliminated if:

1. The inner relation can produce at most one matching row for each outer relation row (uniqueness/distinctness)
2. No attributes from the inner relation are needed above the join level
3. No PlaceHolderVars require the inner relation to be evaluated

The function performs comprehensive checks including:
- Verifying the join is a left join to a single base relation
- Ensuring the inner relation is not the query's result relation (important for MERGE operations)
- Checking that the inner relation supports distinctness proofs
- Verifying no inner relation attributes are referenced above the join
- Analyzing PlaceHolderVar dependencies
- Collecting mergejoinable equality clauses that can prove distinctness
- Attempting to prove the inner relation is distinct for the collected clauses

## Parameters / Member Variables
- : PlannerInfo structure containing all planner context and state information
- : SpecialJoinInfo structure describing the specific outer join being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md): Extracts single member from bitmap set
  - [find_base_rel](../f/find_base_rel.md): Locates base relation information
  - [rel_supports_distinctness](../r/rel_supports_distinctness.md): Checks if relation can have distinctness proven
  - [bms_union](../b/bms_union.md), bms_copy, bms_add_member: Bitmap set operations
  - [bms_is_subset](../b/bms_is_subset.md), bms_overlap, bms_is_member: Bitmap set comparisons
  - [pull_varnos](../p/pull_varnos.md): Extracts variable relation IDs from expressions
  - [clause_sides_match_join](../c/clause_sides_match_join.md): Validates clause structure for join
  - [rel_is_distinct_for](../r/rel_is_distinct_for.md): Attempts to prove relation distinctness
  - RINFO_IS_PUSHED_DOWN: Macro to check if restriction is pushed down

- Called from (representative examples):
  - [remove_useless_joins](../r/remove_useless_joins.md): Main join elimination function

## Notes and Other Information
- Only handles left joins to single base relations; other join types and complex structures are not supported
- The function includes special handling for MERGE operations by preventing elimination of joins to result relations
- [PlaceHolderVar](../P/PlaceHolderVar.md) analysis is particularly complex due to the need to ensure evaluation locations remain valid after join removal
- Uses mergejoinable clauses as the basis for distinctness proofs, as these behave like equality for btree operations
- Includes optimizations like starting attribute checks from max_attr and counting down, assuming system attributes are less likely to be referenced
- The distinctness proof is currently the only method implemented, though comments suggest future extensions for other proof methods

## Simplified Source

```c
static bool join_is_removable(PlannerInfo *root, SpecialJoinInfo *sjinfo)
{
    // Must be left join to single base relation
    if (sjinfo->jointype != JOIN_LEFT ||
        !bms_get_singleton_member(sjinfo->min_righthand, &innerrelid))
        return false;

    // Don't eliminate joins to query result relation (important for MERGE)
    if (innerrelid == root->parse->resultRelation)
        return false;

    innerrel = find_base_rel(root, innerrelid);

    // Quick check: does relation support distinctness proofs?
    if (!rel_supports_distinctness(root, innerrel))
        return false;

    // Calculate join relation IDs
    inputrelids = bms_union(sjinfo->min_lefthand, sjinfo->min_righthand);
    joinrelids = bms_add_member(bms_copy(inputrelids), sjinfo->ojrelid);

    // Check if any inner relation attributes are needed above the join
    for (attroff = innerrel->max_attr - innerrel->min_attr; attroff >= 0; attroff--) {
        if (!bms_is_subset(innerrel->attr_needed[attroff], inputrelids))
            return false;
    }

    // Check PlaceHolderVar dependencies
    foreach(l, root->placeholder_list) {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

        // Various PHV dependency checks...
        if (bms_overlap(phinfo->ph_lateral, innerrel->relids) ||
            (bms_overlap(phinfo->ph_eval_at, innerrel->relids) &&
             !bms_is_subset(phinfo->ph_needed, inputrelids) &&
             /* additional complex PHV checks */))
            return false;
    }

    // Collect mergejoinable equality clauses
    List *clause_list = NIL;
    foreach(l, innerrel->joininfo) {
        RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);

        // Skip clones, pushed-down clauses, non-mergejoinable clauses
        if (restrictinfo->is_clone ||
            RINFO_IS_PUSHED_DOWN(restrictinfo, joinrelids) ||
            !restrictinfo->can_join ||
            restrictinfo->mergeopfamilies == NIL)
            continue;

        // Check clause structure matches join
        if (clause_sides_match_join(restrictinfo, sjinfo->min_lefthand, innerrel->relids))
            clause_list = lappend(clause_list, restrictinfo);
    }

    // Try to prove inner relation is distinct for these clauses
    return rel_is_distinct_for(root, innerrel, clause_list);
}
```