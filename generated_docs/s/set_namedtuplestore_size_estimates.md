# set_namedtuplestore_size_estimates

## Location
[src/backend/optimizer/path/costsize.c:6005-6037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L6005-L6037)

## Overview
Sets the size estimates for a base relation that represents a named tuplestore reference, using either provided estimates or a default fallback value.

## Definition

```c
structed
 * already.
 *
 * We set the same fields as set_baserel_size_estimates.
 */
void
set_result_size_estimates(PlannerInfo *root, RelOptInfo *rel)
{
	/* Should only be applied to RTE_RESULT base relations */
	Assert(rel->relid > 0);
	Assert(planner_rt_fetch(rel->relid, root)->rtekind == RTE_RESULT);

	/* RTE_RESULT always generates a single row, natively */
	rel->tuples = 1;

	/* Now estimate number of output rows, etc */
	set_baserel_size_estimates(root, rel);
}

/*
 * set_foreign_size_estimates
 *		Set the size estimates for a base relation that is a foreign table.
 *
 * There is not a whole lot that we can do here;
```
## Detailed Description
This function estimates cardinality for relations that reference named tuplestores (Ephemeral Named Relations or ENRs), which are temporary result sets that can be referenced multiple times within a query. The function attempts to use the tuple count provided by the code generating the named tuplestore via the  field. If no valid estimate is available (indicated by a negative value), it falls back to a default estimate of 1000 rows.

Named tuplestores are commonly used in scenarios like stored procedures, triggers, or other contexts where intermediate results need to be materialized and referenced multiple times. The estimation strategy acknowledges that actual row counts may be known in some cases, while in others a typical or reusable estimate is more appropriate for plan caching and reuse.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and global information
- : RelOptInfo structure representing the named tuplestore relation being sized, must be a base relation with NAMEDTUPLESTORE RTE

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - RTE_NAMEDTUPLESTORE
  - [set_baserel_size_estimates](set_baserel_size_estimates.md)
- Called from (representative examples):
  - [set_namedtuplestore_pathlist](set_namedtuplestore_pathlist.md)

## Notes and Other Information
- Uses the  field from the range table entry when available (>= 0)
- Falls back to a default estimate of 1000 rows when no specific estimate is provided
- Should only be applied to base relations with RTE_NAMEDTUPLESTORE range table entry type
- Supports both exact counts (when available) and heuristic estimates for plan reuse scenarios
- The fallback value of 1000 represents a reasonable middle ground for unknown tuplestore sizes
- Uses assertions to verify proper relation type before processing