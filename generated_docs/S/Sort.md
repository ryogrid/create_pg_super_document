# Sort

## Location
[src/include/nodes/plannodes.h:931-949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L931-L949)

## Overview
Sort is a plan node structure that represents sorting operations in PostgreSQL's query execution tree, responsible for ordering tuples according to specified sort keys and criteria.

## Definition

```c
typedef struct Sort
{
	Plan		plan;

	/* number of sort-key columns */
	int			numCols;

	/* their indexes in the target list */
	AttrNumber *sortColIdx pg_node_attr(array_size(numCols));

	/* OIDs of operators to sort them by */
	Oid		   *sortOperators pg_node_attr(array_size(numCols));

	/* OIDs of collations */
	Oid		   *collations pg_node_attr(array_size(numCols));

	/* NULLS FIRST/LAST directions */
	bool	   *nullsFirst pg_node_attr(array_size(numCols));
} Sort;
```
## Detailed Description
The Sort node is a fundamental plan node in PostgreSQL's execution framework that implements tuple sorting functionality. It inherits from the base Plan structure and extends it with sort-specific metadata. The Sort node maintains arrays of sort keys, operators, collations, and null handling directives that define how the sorting operation should be performed. This node is created by the planner when ORDER BY clauses, merge joins, or other operations require sorted input.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : Number of columns to sort by
- : Array of attribute numbers indicating which columns from the target list to sort by
- : Array of OIDs identifying the comparison operators for each sort column
- : Array of OIDs specifying the collation to use for each sort column
- : Array of boolean values indicating whether NULL values should sort first (true) or last (false) for each column

## Dependencies
- Functions called/Symbols referenced:
  - Plan (base structure)
  - AttrNumber
  - Oid

- Called from (representative examples):
  - ExecSort (executor/nodeSort.c:77)
  - ExecInitSort (executor/nodeSort.c:221)
  - create_sort_plan (optimizer/plan/createplan.c:2183)
  - make_sort (optimizer/plan/createplan.c:6073)
  - create_mergejoin_plan (optimizer/plan/createplan.c:4530)
  - show_sort_keys (commands/explain.c:2561)

## Notes and Other Information
- The Sort node is used extensively throughout PostgreSQL's execution engine for implementing ORDER BY clauses, preparing input for merge joins, and supporting various aggregate operations
- Sort operations can be memory-intensive and may spill to disk when the work_mem setting is exceeded
- The node supports multi-column sorting with different operators and collations for each column
- NULL handling can be specified independently for each sort column
- Related to IncrementalSort which provides optimized sorting for partially pre-sorted input