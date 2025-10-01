# populate_joinrel_with_paths

## Location
[src/backend/optimizer/path/joinrels.c:894-1071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L894-L1071)

## Overview
Adds execution paths to a join relation by considering all possible join types and optimizations, handling dummy relations and constant-false restrictions.

## Definition

```c
struction paths.)
	 *
	 * Also, a provably constant-false join restriction typically means that
	 * we can skip evaluating one or both sides of the join.  We do this by
	 * marking the appropriate rel as dummy.  For outer joins, a
	 * constant-false restriction that is pushed down still means the whole
	 * join is dummy, while a non-pushed-down one means that no inner rows
	 * will join so we can treat the inner rel as dummy.
	 *
	 * We need only consider the jointypes that appear in join_info_list, plus
	 * JOIN_INNER.
	 */
	switch (sjinfo->jointype)
	{
		case JOIN_INNER:
			if (is_dummy_rel(rel1) || is_dummy_rel(rel2) ||
				restriction_is_constant_false(restrictlist, joinrel, false))
			{
				mark_dummy_rel(joinrel);
				break;
			}
			add_paths_to_joinrel(root, joinrel, rel1, rel2,
								 JOIN_INNER, sjinfo,
								 restrictlist);
			add_paths_to_joinrel(root, joinrel, rel2, rel1,
								 JOIN_INNER, sjinfo,
								 restrictlist);
			break;
		case JOIN_LEFT:
			if (is_dummy_rel(rel1) ||
				restriction_is_constant_false(restrictlist, joinrel, true))
			{
				mark_dummy_rel(joinrel);
				break;
			}
			if (restriction_is_constant_false(restrictlist, joinrel, false) &&
				bms_is_subset(rel2->relids, sjinfo->syn_righthand))
				mark_dummy_rel(rel2);
			add_paths_to_joinrel(root, joinrel, rel1, rel2,
								 JOIN_LEFT, sjinfo,
								 restrictlist);
			add_paths_to_joinrel(root, joinrel, rel2, rel1,
								 JOIN_RIGHT, sjinfo,
								 restrictlist);
			break;
		case JOIN_FULL:
			if ((is_dummy_rel(rel1) && is_dummy_rel(rel2)) ||
				restriction_is_constant_false(restrictlist, joinrel, true))
			{
				mark_dummy_rel(joinrel);
				break;
			}
			add_paths_to_joinrel(root, joinrel, rel1, rel2,
								 JOIN_FULL, sjinfo,
								 restrictlist);
			add_paths_to_joinrel(root, joinrel, rel2, rel1,
								 JOIN_FULL, sjinfo,
								 restrictlist);

			/*
			 * If there are join quals that aren't mergeable or hashable, we
			 * may not be able to build any valid plan.  Complain here so that
			 * we can give a somewhat-useful error message.  (Since we have no
			 * flexibility of planning for a full join, there's no chance of
			 * succeeding later with another pair of input rels.)
			 */
			if (joinrel->pathlist == NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions")));
			break;
		case JOIN_SEMI:

			/*
			 * We might have a normal semijoin, or a case where we don't have
			 * enough rels to do the semijoin but can unique-ify the RHS and
			 * then do an innerjoin (see comments in join_is_legal).  In the
			 * latter case we can't apply JOIN_SEMI joining.
			 */
			if (bms_is_subset(sjinfo->min_lefthand, rel1->relids) &&
				bms_is_subset(sjinfo->min_righthand, rel2->relids))
			{
				if (is_dummy_rel(rel1) || is_dummy_rel(rel2) ||
					restriction_is_constant_false(restrictlist, joinrel, false))
				{
					mark_dummy_rel(joinrel);
					break;
				}
				add_paths_to_joinrel(root, joinrel, rel1, rel2,
									 JOIN_SEMI, sjinfo,
									 restrictlist);
			}

			/*
			 * If we know how to unique-ify the RHS and one input rel is
			 * exactly the RHS (not a superset) we can consider unique-ifying
			 * it and then doing a regular join.  (The create_unique_path
			 * check here is probably redundant with what join_is_legal did,
			 * but if so the check is cheap because it's cached.  So test
			 * anyway to be sure.)
			 */
			if (bms_equal(sjinfo->syn_righthand, rel2->relids) &&
				create_unique_path(root, rel2, rel2->cheapest_total_path,
								   sjinfo) != NULL)
			{
				if (is_dummy_rel(rel1) || is_dummy_rel(rel2) ||
					restriction_is_constant_false(restrictlist, joinrel, false))
				{
					mark_dummy_rel(joinrel);
					break;
				}
				add_paths_to_joinrel(root, joinrel, rel1, rel2,
									 JOIN_UNIQUE_INNER, sjinfo,
									 restrictlist);
				add_paths_to_joinrel(root, joinrel, rel2, rel1,
									 JOIN_UNIQUE_OUTER, sjinfo,
									 restrictlist);
			}
			break;
		case JOIN_ANTI:
			if (is_dummy_rel(rel1) ||
				restriction_is_constant_false(restrictlist, joinrel, true))
			{
				mark_dummy_rel(joinrel);
				break;
			}
			if (restriction_is_constant_false(restrictlist, joinrel, false) &&
				bms_is_subset(rel2->relids, sjinfo->syn_righthand))
				mark_dummy_rel(rel2);
			add_paths_to_joinrel(root, joinrel, rel1, rel2,
								 JOIN_ANTI, sjinfo,
								 restrictlist);
			add_paths_to_joinrel(root, joinrel, rel2, rel1,
								 JOIN_RIGHT_ANTI, sjinfo,
								 restrictlist);
			break;
		default:
			/* other values not expected here */
			elog(ERROR, "unrecognized join type: %d", (int) sjinfo->jointype);
			break;
	}

	/* Apply partitionwise join technique, if possible. */
	try_partitionwise_join(root, rel1, rel2, joinrel, sjinfo, restrictlist);
```
## Detailed Description
The  function is the core path generation engine for join relations in PostgreSQL's query optimizer. It systematically considers different join execution strategies based on the join type (INNER, LEFT, FULL, SEMI, ANTI) and creates corresponding execution paths. The function implements sophisticated logic to handle edge cases such as provably empty relations (dummy rels) and constant-false join restrictions that can eliminate entire join branches.

For each join type, the function evaluates whether the join can produce any results and marks relations as dummy when appropriate. It considers both directions of joining (rel1⋈rel2 and rel2⋈rel1) and applies join-specific optimizations. For SEMI joins, it can transform them into regular joins with uniqueness constraints when beneficial. The function also handles partitionwise joining as a final optimization step.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning context
- : First RelOptInfo representing one input relation to the join
- : Second RelOptInfo representing the other input relation to the join  
- : The target RelOptInfo that will receive the generated join paths
- : SpecialJoinInfo containing join type and constraint information
- : List of join clauses and other applicable restrictions for this join pair

## Dependencies
- Functions called/Symbols referenced:
  - [is_dummy_rel](../i/is_dummy_rel.md)
  - [restriction_is_constant_false](../r/restriction_is_constant_false.md)
  - [mark_dummy_rel](../m/mark_dummy_rel.md)
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [create_unique_path](../c/create_unique_path.md)
  - [try_partitionwise_join](../t/try_partitionwise_join.md)
- Called from (representative examples):
  - [make_join_rel](../m/make_join_rel.md)
  - [try_partitionwise_join](../t/try_partitionwise_join.md)

## Notes and Other Information
- Function is static to the joinrels.c file and not exposed externally
- Implements different logic paths for each join type: INNER, LEFT, FULL, SEMI, ANTI
- For FULL joins, validates that merge-joinable or hash-joinable conditions exist, erroring otherwise
- SEMI joins can be converted to UNIQUE_INNER/UNIQUE_OUTER joins when the RHS can be uniquified
- Handles constant-false restrictions differently for pushed-down vs non-pushed-down cases in outer joins
- Always attempts partitionwise joining as a final optimization opportunity
- Critical for ensuring all viable execution paths are considered while avoiding impossible joins

## Simplified Source

```c
static void
populate_joinrel_with_paths(PlannerInfo *root, RelOptInfo *rel1,
                           RelOptInfo *rel2, RelOptInfo *joinrel,
                           SpecialJoinInfo *sjinfo, List *restrictlist)
{
    // Main join path generation based on join type
    switch (sjinfo->jointype)
    {
        case JOIN_INNER:
            // Check for empty relations or impossible conditions
            if (is_dummy_rel(rel1) || is_dummy_rel(rel2) ||
                restriction_is_constant_false(restrictlist, joinrel, false))
            {
                mark_dummy_rel(joinrel);
                break;
            }
            // Add paths for both join directions
            add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_INNER, sjinfo, restrictlist);
            add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_INNER, sjinfo, restrictlist);
            break;

        case JOIN_LEFT:
            // Left join: check if left side is empty
            if (is_dummy_rel(rel1) ||
                restriction_is_constant_false(restrictlist, joinrel, true))
            {
                mark_dummy_rel(joinrel);
                break;
            }
            // Handle constant-false non-pushed-down restrictions
            if (restriction_is_constant_false(restrictlist, joinrel, false) &&
                bms_is_subset(rel2->relids, sjinfo->syn_righthand))
                mark_dummy_rel(rel2);

            add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_LEFT, sjinfo, restrictlist);
            add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_RIGHT, sjinfo, restrictlist);
            break;

        case JOIN_FULL:
            // Full join: both sides must be empty to make join empty
            if ((is_dummy_rel(rel1) && is_dummy_rel(rel2)) ||
                restriction_is_constant_false(restrictlist, joinrel, true))
            {
                mark_dummy_rel(joinrel);
                break;
            }
            add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_FULL, sjinfo, restrictlist);
            add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_FULL, sjinfo, restrictlist);

            // Ensure we have valid join conditions for FULL JOIN
            if (joinrel->pathlist == NIL)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions")));
            break;

        case JOIN_SEMI:
            // Try normal semijoin if we have proper relation subsets
            if (bms_is_subset(sjinfo->min_lefthand, rel1->relids) &&
                bms_is_subset(sjinfo->min_righthand, rel2->relids))
            {
                if (is_dummy_rel(rel1) || is_dummy_rel(rel2) ||
                    restriction_is_constant_false(restrictlist, joinrel, false))
                {
                    mark_dummy_rel(joinrel);
                    break;
                }
                add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_SEMI, sjinfo, restrictlist);
            }

            // Try unique-ifying RHS and doing regular join
            if (bms_equal(sjinfo->syn_righthand, rel2->relids) &&
                create_unique_path(root, rel2, rel2->cheapest_total_path, sjinfo) != NULL)
            {
                if (is_dummy_rel(rel1) || is_dummy_rel(rel2) ||
                    restriction_is_constant_false(restrictlist, joinrel, false))
                {
                    mark_dummy_rel(joinrel);
                    break;
                }
                add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_UNIQUE_INNER, sjinfo, restrictlist);
                add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_UNIQUE_OUTER, sjinfo, restrictlist);
            }
            break;

        case JOIN_ANTI:
            // Anti join: similar to left join logic
            if (is_dummy_rel(rel1) ||
                restriction_is_constant_false(restrictlist, joinrel, true))
            {
                mark_dummy_rel(joinrel);
                break;
            }
            if (restriction_is_constant_false(restrictlist, joinrel, false) &&
                bms_is_subset(rel2->relids, sjinfo->syn_righthand))
                mark_dummy_rel(rel2);

            add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_ANTI, sjinfo, restrictlist);
            add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_RIGHT_ANTI, sjinfo, restrictlist);
            break;

        default:
            elog(ERROR, "unrecognized join type: %d", (int) sjinfo->jointype);
            break;
    }

    // Always try partitionwise join optimization
    try_partitionwise_join(root, rel1, rel2, joinrel, sjinfo, restrictlist);
}
```