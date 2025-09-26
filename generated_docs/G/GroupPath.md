# GroupPath

## Location
[src/include/nodes/pathnodes.h:2225-2231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2225-L2231)

## Overview
GroupPath represents a query execution path node that performs grouping operations on presorted input data, typically used to implement SQL GROUP BY clauses.

## Definition

```c
typedef struct GroupPath
{
	Path		path;
	Path	   *subpath;		/* path representing input source */
	List	   *groupClause;	/* a list of SortGroupClause's */
	List	   *qual;			/* quals (HAVING quals), if any */
} GroupPath;
```
## Detailed Description
GroupPath is a specialized path node in PostgreSQL's query planner that represents grouping operations performed on already sorted input data. It inherits from the base Path structure and adds specific fields needed for grouping operations. The path assumes that the input data is appropriately sorted according to the grouping columns, which allows for efficient streaming grouping without requiring additional sorting.

The GroupPath is designed to handle both simple grouping operations and more complex scenarios involving HAVING clauses. It maintains information about the underlying data source (subpath), the grouping criteria (groupClause), and any post-grouping filters (qual).

## Parameters / Member Variables
- `path`: Base Path structure containing common path information (cost, parent relation, target, etc.)
- `*subpath`: Pointer to the input Path node that provides the source data for grouping
- `*groupClause`: List of SortGroupClause structures defining the columns to group by
- `*qual`: List of qualification expressions representing HAVING clauses to be applied after grouping
## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - [List](../L/List.md) (for groupClause and qual)
  - [SortGroupClause](../S/SortGroupClause.md) (grouping column specifications)
- Called from (representative examples):
  - [create_group_path](../c/create_group_path.md) (creates GroupPath instances)
  - [create_group_plan](../c/create_group_plan.md) (converts GroupPath to execution plan)
  - [create_plan_recurse](../c/create_plan_recurse.md) (part of plan creation process)

## Notes and Other Information
- The input data must be presorted according to the grouping columns for efficient processing
- [GroupPath](GroupPath.md) preserves the sort ordering of its input, making it suitable for chaining with other operations
- Cost estimation is performed by the cost_group function during path creation
- The path supports parallel execution when the underlying subpath is parallel-safe
- [GroupPath](GroupPath.md) is typically created during the upper planning phase when processing GROUP BY clauses