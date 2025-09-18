# PerformCursorOpen

## Location
src/backend/commands/portalcmds.c: 43 - 166

## Overview
PerformCursorOpen executes the SQL DECLARE CURSOR command, creating a named portal that allows for sequential access to query results without loading the entire result set into memory.

## Definition
```c
void PerformCursorOpen(ParseState *pstate, DeclareCursorStmt *cstmt, ParamListInfo params, bool isTopLevel)
```

## Detailed Description
PerformCursorOpen implements the core functionality for the DECLARE CURSOR SQL command. It validates the cursor declaration, performs query rewriting and planning, creates a portal to hold the prepared query, and sets up the cursor options. The function handles both holdable and non-holdable cursors, enforcing transaction requirements appropriately.

The function performs several key operations:
1. Validates the cursor name (must not be empty)
2. Enforces transaction block requirements for non-holdable cursors
3. Applies query rewriting rules to the parsed query
4. Creates and plans the query using the PostgreSQL planner
5. Creates a portal and copies the plan and query string into portal memory
6. Sets up cursor scrolling options based on user preferences and query capabilities
7. Starts portal execution with the provided parameters

The cursor is not actually executed until PerformPortalFetch is called, allowing for lazy evaluation of results.

## Parameters / Member Variables
- `pstate`: ParseState containing parsing context and source text information
- `cstmt`: DeclareCursorStmt containing the parsed DECLARE CURSOR statement with options and query
- `params`: ParamListInfo containing parameter values for parameterized queries
- `isTopLevel`: Boolean indicating whether this command is executed at the top level (affects transaction requirements)

## Dependencies
- Functions called/Symbols referenced:
  - [RequireTransactionBlock](../R/RequireTransactionBlock.md)
  - [InSecurityRestrictedOperation](../I/InSecurityRestrictedOperation.md)
  - [QueryRewrite](../Q/QueryRewrite.md)
  - [pg_plan_query](../p/pg_plan_query.md)
  - CreatePortal
  - [PortalDefineQuery](PortalDefineQuery.md)
  - [copyParamList](../c/copyParamList.md)
  - [ExecSupportsBackwardScan](../E/ExecSupportsBackwardScan.md)
  - [PortalStart](PortalStart.md)
  - GetActiveSnapshot
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Non-holdable cursors require execution within a transaction block
- Holdable cursors cannot be created within security-restricted operations
- The function automatically determines scroll capability based on query structure unless explicitly specified
- Cursors with row marks (FOR UPDATE/SHARE) are always non-scrollable
- The portal strategy is always PORTAL_ONE_SELECT for cursors
- Parameter values are preserved in the portal's memory context for later execution