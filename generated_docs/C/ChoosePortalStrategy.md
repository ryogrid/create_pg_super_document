# ChoosePortalStrategy

## Location
src/backend/tcop/pquery.c: 209 - 325

## Overview
ChoosePortalStrategy analyzes a list of statements and selects the optimal portal execution strategy based on statement characteristics and count.

## Definition
```c
PortalStrategy ChoosePortalStrategy(List *stmts)
```

## Detailed Description
ChoosePortalStrategy examines a list of Query or PlannedStmt nodes to determine the most appropriate portal execution strategy. The function implements a decision tree that considers factors such as the number of statements, command types, presence of modifying CTEs, RETURNING clauses, and utility commands. For single statements, it can choose between PORTAL_ONE_SELECT, PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT strategies. For multiple statements, it evaluates whether PORTAL_ONE_RETURNING is appropriate (exactly one canSetTag statement with RETURNING) or defaults to PORTAL_MULTI_QUERY. The strategy selection affects how PostgreSQL executes and manages the portal lifecycle.

## Parameters / Member Variables
- `stmts`: List of Query or PlannedStmt nodes to analyze for strategy selection

## Dependencies
- Functions called/Symbols referenced:
  - PortalStrategy (return type)
  - List operations (list_length, linitial, foreach, lfirst)
  - Query and PlannedStmt structures
  - UtilityReturnsTuples
  - nodeTag
  - Portal strategy constants (PORTAL_ONE_SELECT, PORTAL_ONE_MOD_WITH, PORTAL_UTIL_SELECT, PORTAL_ONE_RETURNING, PORTAL_MULTI_QUERY)
  - Command type constants (CMD_SELECT, CMD_UTILITY)
- Called from (representative examples):
  - PortalStart
  - PlanCacheComputeResultDesc

## Notes and Other Information
The function handles both Query and PlannedStmt nodes, making it useful for both portal management and plan cache operations. The decision logic prioritizes more specific strategies (like PORTAL_ONE_SELECT) over general ones (PORTAL_MULTI_QUERY). Single-statement portals receive more optimal strategies, while multi-statement scenarios typically fall back to PORTAL_MULTI_QUERY unless they meet specific criteria for PORTAL_ONE_RETURNING. The canSetTag field is crucial for determining which statements contribute to the portal's completion tag.