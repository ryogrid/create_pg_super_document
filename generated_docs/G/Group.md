# Group

## Location
[src/include/nodes/plannodes.h:967-980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L967-L980)

## Overview
Group is a plan node that implements GROUP BY operations without aggregates, designed to eliminate duplicate rows and produce one output row per distinct group from presorted input data.

## Definition

```c
typedef struct Group
{
	Plan		plan;

	/* number of grouping columns */
	int			numCols;

	/* their indexes in the target list */
	AttrNumber *grpColIdx pg_node_attr(array_size(numCols));

	/* equality operators to compare with */
	Oid		   *grpOperators pg_node_attr(array_size(numCols));
	Oid		   *grpCollations pg_node_attr(array_size(numCols));
} Group;
```
## Detailed Description
The Group node is used specifically for queries with GROUP BY clauses that do not contain aggregate functions. It operates on presorted input data and eliminates duplicate rows by comparing consecutive tuples using the specified grouping columns. When the values in the grouping columns change, the node outputs the first tuple of the previous group and begins processing a new group. This approach is efficient because it requires only sequential processing of sorted data without needing to store all group members in memory. The node is distinct from Agg, which handles GROUP BY queries with aggregate functions.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information
- `numCols`: Number of columns to group by
- `pg_node_attr(array_size(numCols))`: Array of attribute numbers indicating which columns from the target list to use for grouping
- `pg_node_attr(array_size(numCols))`: Array of OIDs identifying the equality operators for comparing grouping columns
- `pg_node_attr(array_size(numCols))`: Array of OIDs specifying the collation to use for each grouping column
## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - AttrNumber
  - Oid

- Called from (representative examples):
  - [ExecInitGroup](../E/ExecInitGroup.md) (executor/nodeGroup.c:161)
  - [create_group_plan](../c/create_group_plan.md) (optimizer/plan/createplan.c:2244)
  - [make_group](../m/make_group.md) (optimizer/plan/createplan.c:6678)
  - [show_group_keys](../s/show_group_keys.md) (commands/explain.c:2742)
  - [make_windowagg](../m/make_windowagg.md) (optimizer/plan/createplan.c:6669)

## Notes and Other Information
- The Group node requires its input to be presorted according to the grouping columns, typically provided by a Sort node beneath it
- Unlike Agg nodes, Group nodes do not compute aggregate functions - they simply eliminate duplicates based on grouping columns
- This node is used for queries like 'SELECT DISTINCT col1, col2 FROM table GROUP BY col1, col2' without aggregate functions
- The node processes data in a streaming fashion, making it memory-efficient for large datasets
- [Group](Group.md) nodes are less common than Agg nodes since most GROUP BY queries involve aggregate functions
- The planner may choose Group over Agg when no aggregation is actually required