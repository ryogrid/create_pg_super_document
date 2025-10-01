# contain_exec_param

## Location
[src/backend/optimizer/util/clauses.c:1137-1142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1137-L1142)

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
  - [contain_exec_param_walker](contain_exec_param_walker.md)
- Called from (representative examples):
  - [test_opexpr_is_hashable](../t/test_opexpr_is_hashable.md)
  - Various optimizer functions that need to check parameter dependencies

## Notes and Other Information
- Specifically targets PARAM_EXEC parameters, not other parameter types
- Does not descend into subqueries, focusing on the current query level
- Used primarily in query optimization contexts where parameter dependencies matter
- Part of the parameter analysis infrastructure in PostgreSQL's query planner
- Located in src/backend/optimizer/util/clauses.c at lines 1137-1142
- Returns true if any matching PARAM_EXEC parameter is found

## Simplified Source

```c
bool
contain_exec_param(Node *clause, List *param_ids)
{
    // Delegate to walker function to recursively search the clause tree
    return contain_exec_param_walker(clause, param_ids);
}
```