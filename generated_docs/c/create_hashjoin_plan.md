# create_hashjoin_plan

## Location
[src/backend/optimizer/plan/createplan.c:4747-4935](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4747-L4935)

## Overview
Creates a HashJoin plan node from a HashPath, implementing hash joins where the inner relation is used to build a hash table that is then probed by the outer relation.

## Definition

```c
structed into outer/inner expressions, so they can be computed
	 * separately (inner expressions are used to build the hashtable via Hash,
	 * outer expressions to perform lookups of tuples from HashJoin's outer
	 * plan in the hashtable). Also collect operator information necessary to
	 * build the hashtable.
	 */
	foreach(lc, hashclauses)
	{
		OpExpr	   *hclause = lfirst_node(OpExpr, lc);

		hashoperators = lappend_oid(hashoperators, hclause->opno);
		hashcollations = lappend_oid(hashcollations, hclause->inputcollid);
		outer_hashkeys = lappend(outer_hashkeys, linitial(hclause->args));
		inner_hashkeys = lappend(inner_hashkeys, lsecond(hclause->args));
	}

	/*
	 * Build the hash node and hash join node.
	 */
	hash_plan = make_hash(inner_plan,
						  inner_hashkeys,
						  skewTable,
						  skewColumn,
						  skewInherit);
```
## Detailed Description
This function creates a HashJoin execution plan node from a HashPath. Hash joins are efficient when one relation (typically the smaller inner relation) can fit in a hash table built in memory, which is then probed by the outer relation to find matches. The function creates both a Hash node for building the hash table and a HashJoin node for the actual join operation. It handles hash key extraction from join clauses, sets up skew optimization for single-column joins when statistics are available, and manages batching for large datasets that don't fit in memory. The function also handles parallel execution by setting up shared hash table sizing information.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information
- : HashPath representing the chosen hash join access path with batching and hash clause information

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - IS_OUTER_JOIN
  - [extract_actual_join_clauses](../e/extract_actual_join_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [get_actual_clauses](../g/get_actual_clauses.md)
  - [list_difference](../l/list_difference.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [get_switched_clauses](../g/get_switched_clauses.md)
  - [is_opclause](../i/is_opclause.md)
  - lappend_oid
  - lsecond
  - [make_hash](../m/make_hash.md)
  - [copy_plan_costsize](copy_plan_costsize.md)
  - [make_hashjoin](../m/make_hashjoin.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_join_plan](create_join_plan.md)

## Notes and Other Information
- Hash joins are typically the most efficient join method when one relation is much smaller than the other
- Creates separate Hash and HashJoin nodes - the Hash node builds the hash table from the inner relation
- Implements skew optimization for single-column joins when column statistics indicate data skew
- Handles batching for large datasets that exceed work_mem by spilling to disk
- Supports parallel execution with shared hash tables across multiple workers
- Extracts hash keys and operators needed for the hash table implementation
- Requests small target lists from inputs to minimize memory usage during hash table operations
- Located at src/backend/optimizer/plan/createplan.c:4747-4935
- Part of the JOIN METHODS section of the planner