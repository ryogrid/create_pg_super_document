# MergeJoin

## Location
src/include/nodes/plannodes.h: 833 - 856

## Overview
MergeJoin is a plan node that implements the merge join algorithm, which efficiently joins two sorted input streams by merging them based on equality conditions over their sort keys.

## Definition

```c
typedef struct MergeJoin
{
	Join		join;

	/* Can we skip mark/restore calls? */
	bool		skip_mark_restore;

	/* mergeclauses as expression trees */
	List	   *mergeclauses;

	/* these are arrays, but have the same length as the mergeclauses list: */

	/* per-clause OIDs of btree opfamilies */
	Oid		   *mergeFamilies pg_node_attr(array_size(mergeclauses));

	/* per-clause OIDs of collations */
	Oid		   *mergeCollations pg_node_attr(array_size(mergeclauses));

	/* per-clause ordering (ASC or DESC) */
	int		   *mergeStrategies pg_node_attr(array_size(mergeclauses));

	/* per-clause nulls ordering */
	bool	   *mergeNullsFirst pg_node_attr(array_size(mergeclauses));
} MergeJoin;
```
## Detailed Description
MergeJoin implements the merge join algorithm, one of the three fundamental join algorithms in PostgreSQL (along with nested loop and hash joins). This algorithm is particularly efficient when both input relations are already sorted on the join keys, as it can process both streams in a single pass with O(M+N) complexity.

The algorithm works by simultaneously scanning both sorted input streams and advancing the appropriate stream based on comparison results. When matching tuples are found, they are joined and output. The merge join can handle multiple join clauses, with each clause having its own ordering specification including btree operator family, collation, sort direction, and nulls positioning.

The skip_mark_restore flag optimizes performance by avoiding expensive mark/restore operations when the inner relation has unique values for the join keys, allowing the algorithm to avoid re-scanning portions of the inner stream.

## Parameters / Member Variables
- : Base Join structure containing common join information (plan details, join type, join qualifiers)
- : Boolean flag indicating whether mark/restore calls can be skipped for performance optimization
- : List of expression trees representing the merge join equality conditions
- : Array of btree operator family OIDs, one for each merge clause, defining the ordering semantics
- : Array of collation OIDs specifying the collation to use for each merge clause
- : Array of sort direction indicators (BTLessStrategyNumber or BTGreaterStrategyNumber) for each merge clause
- : Array of boolean flags indicating whether NULL values should be ordered first for each merge clause

## Dependencies
- Functions called/Symbols referenced:
  - Join (inherited base structure)
  - List (for mergeclauses)
  - Oid (for operator families and collations)

- Called from (representative examples):
  - ExplainNode (commands/explain.c:2175)
  - ExecInitNode (executor/execProcnode.c:303)
  - ExecInitMergeJoin (executor/nodeMergejoin.c:1444)
  - create_mergejoin_plan (optimizer/plan/createplan.c:4443)
  - make_mergejoin (optimizer/plan/createplan.c:6042)

## Notes and Other Information
- Both input relations must be sorted according to the merge clauses for the algorithm to work correctly
- The ordering information (families, collations, strategies, nulls positioning) must be consistent between the left and right inputs
- Merge join is often the most efficient join method when appropriate indexes exist or when the inputs are naturally sorted
- The algorithm handles multiple join columns by treating them as a composite sort key
- Mark/restore operations are used to handle cases where multiple tuples on one side match multiple tuples on the other side
- The pg_node_attr annotations ensure proper handling of the variable-length arrays during node operations