# fetch_upper_rel

## Location
[src/backend/optimizer/util/relnode.c:1470-1520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L1470-L1520)

## Overview
Builds or retrieves a RelOptInfo for post-scan/join query processing operations, known as "upper" relations.

## Definition

```c
structure is just a List for each
	 * relation kind.  If we ever get so many of one kind that this stops
	 * working well, we can improve it.  No code outside this function should
	 * assume anything about how to find a particular upperrel.
	 */

	/* If we already made this upperrel for the query, return it */
	foreach(lc, root->upper_rels[kind])
	{
		upperrel = (RelOptInfo *) lfirst(lc);

		if (bms_equal(upperrel->relids, relids))
			return upperrel;
	}

	upperrel = makeNode(RelOptInfo);
```
## Detailed Description
The  function manages RelOptInfo structures for upper-level query processing operations that occur after basic scanning and joining. These "upper" relations represent processing steps like grouping, windowing, ordering, and set operations.

The function first searches the existing upper_rels list for the specified kind to see if a matching relation already exists. If found, it returns the existing RelOptInfo. If not found, it creates a new RelOptInfo with appropriate initialization for upper-level processing.

Upper relations are identified by an UpperRelationKind enum value and a Relids set. The meaning of the Relids set varies depending on the specific relation kind. Most fields in upper-level RelOptInfo structures are not used and remain zero-initialized, with the function focusing only on fields relevant to path management.

## Parameters / Member Variables
- : PlannerInfo structure containing global query planning state
- : UpperRelationKind enum specifying the type of upper relation (grouping, window, etc.)
- : Relids set identifying the specific upper relation (meaning varies by kind)

## Dependencies
- Functions called/Symbols referenced:
  - UpperRelationKind
  - bms_equal
  - RELOPT_UPPER_REL
  - bms_copy
  - create_empty_pathtarget
- Called from (representative examples):
  - set_subquery_pathlist
  - standard_planner
  - subquery_planner
  - grouping_planner
  - make_grouping_rel
  - create_window_paths
  - create_distinct_paths
  - create_ordered_paths

## Notes and Other Information
- The function uses a simple List-based indexing structure for each relation kind, which could be optimized if performance becomes an issue
- Only fields relevant to add_path() and set_cheapest() are properly initialized
- The consider_startup flag is set based on whether tuple_fraction > 0, indicating partial result retrieval
- Parallel processing consideration is initially disabled but may be enabled later
- The function operates at lines 1470-1520 in src/backend/optimizer/util/relnode.c
- This is a key function in PostgreSQL's upper-level query planning infrastructure