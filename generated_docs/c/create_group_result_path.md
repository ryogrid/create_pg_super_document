# create_group_result_path

## Location
[src/backend/optimizer/util/pathnode.c:1518-1565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1518-L1565)

## Overview
Creates a path representing a Result-and-nothing-else plan for degenerate grouping cases that need to produce exactly one result row.

## Definition


## Detailed Description
This function constructs a GroupResultPath node for degenerate grouping scenarios where the optimizer knows it needs to produce exactly one result row, possibly filtered by a HAVING qualification. This typically occurs in queries with aggregate functions but no GROUP BY clause, or when grouping produces exactly one group. The function creates a Result plan node that generates a single tuple with the computed target expressions.

The cost calculation is specialized since it doesn't use the standard cost_resultscan() function. Instead, it manually calculates costs based on the target expressions and any HAVING qualifications, assuming exactly one output row regardless of the HAVING clause selectivity.

## Parameters / Member Variables
- : PlannerInfo context for the query being planned
- : RelOptInfo for the relation this path represents (typically a grouped relation)
- : PathTarget specifying the output columns and expressions to compute
- : Optional list of HAVING qualification expressions to apply

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (GroupResultPath creation)
  - [cost_qual_eval](cost_qual_eval.md) (for costing HAVING qualifications)
  - cpu_tuple_cost (global cost parameter)
- Called from (representative examples):
  - [create_degenerate_grouping_paths](create_degenerate_grouping_paths.md)
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- Always produces exactly one row (path.rows = 1)
- Never parallel-aware (parallel_aware = false, parallel_workers = 0)
- Has no pathkeys (pathkeys = NIL) since it produces a single tuple
- No param_info since there are no other relations involved
- Cost calculation includes target expression costs plus cpu_tuple_cost
- HAVING qualifications are evaluated once at startup, added to both startup and total costs
- Used for queries like "SELECT COUNT(*) FROM table" or "SELECT SUM(col) FROM table HAVING SUM(col) > 0"
- The selectivity of HAVING quals is ignored since the row count remains 1
- Represents a plan that computes aggregate results without any input scanning