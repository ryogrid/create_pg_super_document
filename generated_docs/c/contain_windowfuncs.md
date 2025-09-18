# contain_windowfuncs

## Location
[src/backend/rewrite/rewriteManip.c:216-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L216-L228)

## Overview
Checks if an expression contains a window function call at the current query level, providing a simple boolean test for window function presence.

## Definition
```c
bool contain_windowfuncs(Node *node)
```

## Detailed Description
This function serves as a detection mechanism for window functions within PostgreSQL's expression trees. Unlike the aggregate detection functions that can check multiple query levels, this function specifically focuses on window functions at the current query level only.

The function is designed with simplicity in mind, taking only a node parameter and using no context structure since it only needs to detect presence at the current level. It leverages the standard query_or_expression_tree_walker pattern to traverse both Query nodes and bare expression trees efficiently.

Window functions (such as ROW_NUMBER(), RANK(), LAG(), LEAD(), etc.) have special execution requirements and restrictions in SQL, making their detection crucial for query planning, optimization, and validation phases. This function provides a fast path for determining if such functions are present without needing the more complex level-aware detection used for aggregates.

## Parameters / Member Variables
- `node`: The root node of the expression tree or Query structure to examine for window functions

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_walker (tree traversal utility)
  - [contain_windowfuncs_walker](contain_windowfuncs_walker.md) (callback function for tree walking)
- Called from (representative examples):
  - [contain_window_function](contain_window_function.md) (optimizer clauses utility)
  - [transformWindowFuncCall](../t/transformWindowFuncCall.md) (parser window function handling)
  - [checkTargetlistEntrySQL92](checkTargetlistEntrySQL92.md) (SQL standard compliance checking)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:216-228
- Simpler API compared to aggregate detection functions (no level parameter needed)
- Uses NULL context since no level tracking is required for current-level detection
- Part of PostgreSQL's query rewrite and analysis infrastructure
- Critical for enforcing window function placement rules and optimization decisions
- Window functions have different execution semantics than regular aggregates, requiring separate detection logic