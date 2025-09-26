# PortalGetPrimaryStmt

## Location
src/backend/utils/mmgr/portalmem.c: 151 - 174

## Overview
Retrieves the primary PlannedStmt from a portal's statement list, specifically the statement that is marked with canSetTag flag.

## Definition
```c
PlannedStmt *PortalGetPrimaryStmt(Portal portal)
```

## Detailed Description
PortalGetPrimaryStmt searches through a portal's list of planned statements to find the "primary" statement - the one that can set command tags. The canSetTag flag identifies statements that are responsible for generating command completion information (like "SELECT 5" or "INSERT 0 1") that gets returned to the client.

The function iterates through the portal's stmts list and returns the first PlannedStmt that has its canSetTag field set to true. In typical PostgreSQL usage, there should be exactly one such statement per portal, though the function handles edge cases by returning the first match or NULL if none exists.

This function is essential for query execution infrastructure, particularly for determining which statement within a multi-statement portal should provide the command tag and result metadata.

## Parameters / Member Variables
- `portal`: Portal object containing the list of planned statements to search through

## Dependencies
- Functions called/Symbols referenced:
  - PlannedStmt: Planned statement structure containing canSetTag field
  - lfirst_node: List traversal macro for accessing PlannedStmt nodes
  - foreach: List iteration macro

- Called from:
  - FetchPortalTargetList: Retrieving target list information for portals
  - PortalStart: Portal execution initialization

## Notes and Other Information
- Returns NULL if no statement has canSetTag set to true
- If multiple statements have canSetTag=true, returns the first one found
- According to comments, neither edge case should occur in normal usage
- The canSetTag flag determines which statement provides command completion tags
- Critical for proper command tag generation in query result reporting
- Used primarily during portal execution setup and result metadata determination