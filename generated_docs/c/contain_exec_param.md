# contain_exec_param

## Location
src/backend/optimizer/util/clauses.c: 1137 - 1142

## Overview
A function that searches for PARAM_EXEC parameters with specific parameter IDs within a clause expression tree.

## Definition
```c
bool contain_exec_param(Node *clause, List *param_ids)
```

## Detailed Description
This function provides a high-level interface for detecting the presence of PARAM_EXEC parameters within PostgreSQL expression trees. It specifically searches for execution-time parameters (PARAM_EXEC) that have parameter IDs matching those provided in the param_ids list. The function is used during query planning and optimization to determine parameter dependencies, particularly important for subquery planning and execution strategy decisions. Unlike general parameter detection, this function specifically targets execution parameters and does not descend into subqueries, making it suitable for analyzing parameter dependencies at the current query level.

## Parameters / Member Variables
- `clause`: The expression node tree to search for PARAM_EXEC parameters
- `param_ids`: A list of parameter IDs to match against when searching for PARAM_EXEC parameters

## Dependencies
- Functions called/Symbols referenced:
  - contain_exec_param_walker
- Called from (representative examples):
  - test_opexpr_is_hashable
  - Various optimizer functions that need to check parameter dependencies

## Notes and Other Information
- Specifically targets PARAM_EXEC parameters, not other parameter types
- Does not descend into subqueries, focusing on the current query level
- Used primarily in query optimization contexts where parameter dependencies matter
- Part of the parameter analysis infrastructure in PostgreSQL's query planner
- Located in src/backend/optimizer/util/clauses.c at lines 1137-1142
- Returns true if any matching PARAM_EXEC parameter is found