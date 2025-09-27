# ChoosePortalStrategy

## Location
[src/backend/tcop/pquery.c:209-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L209-L325)

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
  - [List](../L/List.md) operations (list_length, linitial, foreach, lfirst)
  - [Query](../Q/Query.md) and PlannedStmt structures
  - [UtilityReturnsTuples](../U/UtilityReturnsTuples.md)
  - nodeTag
  - [Portal](../P/Portal.md) strategy constants (PORTAL_ONE_SELECT, PORTAL_ONE_MOD_WITH, PORTAL_UTIL_SELECT, PORTAL_ONE_RETURNING, PORTAL_MULTI_QUERY)
  - [Command](Command.md) type constants (CMD_SELECT, CMD_UTILITY)
- Called from (representative examples):
  - [PortalStart](../P/PortalStart.md)
  - [PlanCacheComputeResultDesc](../P/PlanCacheComputeResultDesc.md)

## Notes and Other Information
The function handles both Query and PlannedStmt nodes, making it useful for both portal management and plan cache operations. The decision logic prioritizes more specific strategies (like PORTAL_ONE_SELECT) over general ones (PORTAL_MULTI_QUERY). Single-statement portals receive more optimal strategies, while multi-statement scenarios typically fall back to PORTAL_MULTI_QUERY unless they meet specific criteria for PORTAL_ONE_RETURNING. The canSetTag field is crucial for determining which statements contribute to the portal's completion tag.

## Simplified Source

```c
// Simplified version of ChoosePortalStrategy
PortalStrategy ChoosePortalStrategy(List *stmts) {
    int nSetTag;
    ListCell *lc;

    // Handle single statement case
    if (list_length(stmts) == 1) {
        Node *stmt = (Node *) linitial(stmts);

        if (IsA(stmt, Query)) {
            Query *query = (Query *) stmt;

            if (query->canSetTag) {
                if (query->commandType == CMD_SELECT) {
                    // SELECT with modifying CTE vs regular SELECT
                    return query->hasModifyingCTE ?
                           PORTAL_ONE_MOD_WITH : PORTAL_ONE_SELECT;
                }
                if (query->commandType == CMD_UTILITY) {
                    // Utility command that returns tuples vs doesn't
                    return UtilityReturnsTuples(query->utilityStmt) ?
                           PORTAL_UTIL_SELECT : PORTAL_MULTI_QUERY;
                }
            }
        }
        else if (IsA(stmt, PlannedStmt)) {
            PlannedStmt *pstmt = (PlannedStmt *) stmt;

            if (pstmt->canSetTag) {
                if (pstmt->commandType == CMD_SELECT) {
                    // SELECT with modifying CTE vs regular SELECT
                    return pstmt->hasModifyingCTE ?
                           PORTAL_ONE_MOD_WITH : PORTAL_ONE_SELECT;
                }
                if (pstmt->commandType == CMD_UTILITY) {
                    // Utility command that returns tuples vs doesn't
                    return UtilityReturnsTuples(pstmt->utilityStmt) ?
                           PORTAL_UTIL_SELECT : PORTAL_MULTI_QUERY;
                }
            }
        }
        else {
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(stmt));
        }
    }

    // Handle multiple statements: check for PORTAL_ONE_RETURNING
    nSetTag = 0;
    foreach(lc, stmts) {
        Node *stmt = (Node *) lfirst(lc);

        if (IsA(stmt, Query)) {
            Query *query = (Query *) stmt;

            if (query->canSetTag) {
                if (++nSetTag > 1) {
                    return PORTAL_MULTI_QUERY;  // Too many tag-setting statements
                }
                // Must be non-utility with RETURNING clause
                if (query->commandType == CMD_UTILITY ||
                    query->returningList == NIL) {
                    return PORTAL_MULTI_QUERY;
                }
            }
        }
        else if (IsA(stmt, PlannedStmt)) {
            PlannedStmt *pstmt = (PlannedStmt *) stmt;

            if (pstmt->canSetTag) {
                if (++nSetTag > 1) {
                    return PORTAL_MULTI_QUERY;  // Too many tag-setting statements
                }
                // Must be non-utility with RETURNING clause
                if (pstmt->commandType == CMD_UTILITY ||
                    !pstmt->hasReturning) {
                    return PORTAL_MULTI_QUERY;
                }
            }
        }
        else {
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(stmt));
        }
    }

    // Exactly one statement with RETURNING clause
    if (nSetTag == 1) {
        return PORTAL_ONE_RETURNING;
    }

    // Default case
    return PORTAL_MULTI_QUERY;
}
```

Key simplifications made:
- Added clear comments explaining the decision logic for each strategy
- Consolidated similar Query and PlannedStmt handling logic
- Highlighted the key criteria for each portal strategy
- Simplified the nested conditionals with clear explanations
- Focused on the main decision points: single vs multiple statements, command types, and RETURNING clauses