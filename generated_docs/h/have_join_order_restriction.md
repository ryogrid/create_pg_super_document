# have_join_order_restriction

## Location
[src/backend/optimizer/path/joinrels.c:1072-1184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1072-L1184)

## Overview
Detects whether two relations should be joined to satisfy join-order restrictions from special joins, lateral references, or PlaceHolderVar requirements.

## Definition

```c
struct a plan at all.)
	 */
	foreach(l, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

		if (bms_is_subset(rel1->relids, phinfo->ph_eval_at) &&
			bms_is_subset(rel2->relids, phinfo->ph_eval_at))
			return true;
	}

	/*
	 * It's possible that the rels correspond to the left and right sides of a
	 * degenerate outer join, that is, one with no joinclause mentioning the
	 * non-nullable side;
```
## Detailed Description
The  function determines whether a join between two relations is required to satisfy various ordering constraints in query execution. It handles several types of mandatory join situations: lateral references between relations, PlaceHolderVar computation requirements, and degenerate outer joins that lack explicit join clauses but must still be executed due to semantic requirements.

The function implements a critical optimization heuristic by deferring clauseless bushy joins when possible. This prevents the optimizer from wasting effort on inefficient join combinations when join-order restrictions exist high in the join tree. The function returns false if either input relation can legally join with other relations using actual join clauses, effectively prioritizing joins with explicit conditions over purely structural joins.

## Parameters
- `root`: The PlannerInfo structure containing global query planning context including join_info_list and placeholder_list
- `rel1`: First RelOptInfo to be considered for joining
- `rel2`: Second RelOptInfo to be considered for joining

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [has_legal_joinclause](has_legal_joinclause.md)
- Called from (representative examples):
  - [join_search_one_level](../j/join_search_one_level.md)
  - [make_rels_by_clause_joins](../m/make_rels_by_clause_joins.md)
  - [desirable_join](../d/desirable_join.md)

## Notes and Other Information
- Always used in conjunction with have_relevant_joinclause() in practice, though kept separate for clarity
- Handles degenerate cases where clauseless joins must be performed for join-order restrictions
- Returns true immediately if either relation has a direct lateral reference to the other
- Considers PlaceHolderVar eval_at requirements that span both relations
- Ignores full joins as they are handled by other mechanisms
- Uses overlap tests rather than subset tests when checking for partial SJ completion needs
- Critical for ensuring plan construction succeeds in complex join scenarios with outer joins and subqueries
- Implements important bushy join deferral optimization to avoid combinatorial explosion in join search

## Simplified Source

```c
bool
have_join_order_restriction(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2)
{
    bool result = false;
    ListCell *l;

    // Check for direct lateral references between the relations
    if (bms_overlap(rel1->relids, rel2->direct_lateral_relids) ||
        bms_overlap(rel2->relids, rel1->direct_lateral_relids))
        return true;

    // Check if both relations are needed for some PlaceHolderVar
    foreach(l, root->placeholder_list)
    {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

        if (bms_is_subset(rel1->relids, phinfo->ph_eval_at) &&
            bms_is_subset(rel2->relids, phinfo->ph_eval_at))
            return true;
    }

    // Check special join constraints
    foreach(l, root->join_info_list)
    {
        SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(l);

        // Skip full joins - handled elsewhere
        if (sjinfo->jointype == JOIN_FULL)
            continue;

        // Check if we can perform the special join with these relations
        if ((bms_is_subset(sjinfo->min_lefthand, rel1->relids) &&
             bms_is_subset(sjinfo->min_righthand, rel2->relids)) ||
            (bms_is_subset(sjinfo->min_lefthand, rel2->relids) &&
             bms_is_subset(sjinfo->min_righthand, rel1->relids)))
        {
            result = true;
            break;
        }

        // Check if we need to join these to complete LHS or RHS
        if ((bms_overlap(sjinfo->min_righthand, rel1->relids) &&
             bms_overlap(sjinfo->min_righthand, rel2->relids)) ||
            (bms_overlap(sjinfo->min_lefthand, rel1->relids) &&
             bms_overlap(sjinfo->min_lefthand, rel2->relids)))
        {
            result = true;
            break;
        }
    }

    // Defer clauseless bushy joins when possible
    if (result)
    {
        if (has_legal_joinclause(root, rel1) ||
            has_legal_joinclause(root, rel2))
            result = false;
    }

    return result;
}
```