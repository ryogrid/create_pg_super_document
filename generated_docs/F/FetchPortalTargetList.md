# FetchPortalTargetList

## Location
[src/backend/tcop/pquery.c:326-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L326-L347)

## Overview
FetchPortalTargetList extracts the target list from a portal that returns tuples, providing information about the columns and expressions in the query result.

## Definition
```c
List *FetchPortalTargetList(Portal portal)
```

## Detailed Description
FetchPortalTargetList retrieves the target list (list of output columns and expressions) from a portal that is designed to return tuples. The function first checks if the portal strategy is suitable for returning tuples - it immediately returns NIL for PORTAL_MULTI_QUERY strategy since such portals don't have a determinable target list. For other portal strategies, it delegates to FetchStatementTargetList to extract the target list from the portal's primary statement. The returned list describes the structure and types of columns that the portal will produce when executed.

## Parameters / Member Variables
- `portal`: The portal from which to extract the target list

## Dependencies
- Functions called/Symbols referenced:
  - [Portal](../P/Portal.md) (parameter type)
  - PORTAL_MULTI_QUERY (strategy constant)
  - [FetchStatementTargetList](FetchStatementTargetList.md)
  - PortalGetPrimaryStmt
  - NIL (empty list constant)
- Called from (representative examples):
  - [printtup_startup](../p/printtup_startup.md)
  - [exec_describe_portal_message](../e/exec_describe_portal_message.md)
  - [FetchStatementTargetList](FetchStatementTargetList.md)

## Notes and Other Information
The function returns NIL (empty list) if the portal doesn't have a determinable target list, which occurs for PORTAL_MULTI_QUERY strategies. The returned list should not be modified by the caller as indicated in the function comments. This function is essential for describing portal results to clients and setting up appropriate output formatting. The target list contains TargetEntry nodes that describe each output column's name, type, and expression.