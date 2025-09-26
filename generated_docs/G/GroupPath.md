# GroupPath

## Location
src/include/nodes/pathnodes.h: 2225 - 2231

## Overview
GroupPath represents a query execution path node that performs grouping operations on presorted input data, typically used to implement SQL GROUP BY clauses.

## Definition


## Detailed Description
GroupPath is a specialized path node in PostgreSQL's query planner that represents grouping operations performed on already sorted input data. It inherits from the base Path structure and adds specific fields needed for grouping operations. The path assumes that the input data is appropriately sorted according to the grouping columns, which allows for efficient streaming grouping without requiring additional sorting.

The GroupPath is designed to handle both simple grouping operations and more complex scenarios involving HAVING clauses. It maintains information about the underlying data source (subpath), the grouping criteria (groupClause), and any post-grouping filters (qual).

## Parameters / Member Variables
- : Base Path structure containing common path information (cost, parent relation, target, etc.)
- : Pointer to the input Path node that provides the source data for grouping
- : List of SortGroupClause structures defining the columns to group by
- : List of qualification expressions representing HAVING clauses to be applied after grouping

## Dependencies
- Functions called/Symbols referenced:
  - Path (base structure)
  - List (for groupClause and qual)
  - SortGroupClause (grouping column specifications)
- Called from (representative examples):
  - create_group_path (creates GroupPath instances)
  - create_group_plan (converts GroupPath to execution plan)
  - create_plan_recurse (part of plan creation process)

## Notes and Other Information
- The input data must be presorted according to the grouping columns for efficient processing
- GroupPath preserves the sort ordering of its input, making it suitable for chaining with other operations
- Cost estimation is performed by the cost_group function during path creation
- The path supports parallel execution when the underlying subpath is parallel-safe
- GroupPath is typically created during the upper planning phase when processing GROUP BY clauses