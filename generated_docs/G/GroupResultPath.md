# GroupResultPath

## Location
[src/include/nodes/pathnodes.h:1969-1973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1969-L1973)

## Overview
GroupResultPath represents use of a Result plan node to compute the output of a degenerate GROUP BY case, producing exactly one row that might be filtered by a HAVING qual.

## Definition
```c
typedef struct GroupResultPath
{
	Path		path;
	List	   *quals;
} GroupResultPath;
```

## Detailed Description
GroupResultPath is a specialized path node used for degenerate GROUP BY operations where the planner knows it needs to produce exactly one result row. This occurs when there are no grouping columns (i.e., only aggregate functions in SELECT) or when all grouping expressions are constants. The path represents a Result plan node that computes aggregate values without scanning any base tables.

This is commonly seen in queries like `SELECT COUNT(*) FROM table` or `SELECT SUM(col) FROM table WHERE condition` where no GROUP BY clause is present or the GROUP BY contains only constants. The Result node generates a single output row, which may then be filtered by HAVING clauses if present.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information including cost estimates and target list
- `quals`: List of bare clauses (not RestrictInfos) representing HAVING conditions that filter the single result row

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [ExecSupportsMarkRestore](../E/ExecSupportsMarkRestore.md)
  - [create_plan_recurse](../c/create_plan_recurse.md)
  - [create_group_result_plan](../c/create_group_result_plan.md)
  - [create_group_result_path](../c/create_group_result_path.md)

## Notes and Other Information
- Used specifically for degenerate grouping cases where exactly one row is guaranteed
- The quals list contains bare clauses, not RestrictInfo structures as in other path types
- Cost calculation assumes single row output regardless of HAVING qual selectivity
- HAVING quals are evaluated once at startup and affect both startup and total costs
- Pathkeys are always NIL since there is only one output row
- Cannot be parallelized since it produces only one row
- The Result plan node it represents generates output without scanning base relations