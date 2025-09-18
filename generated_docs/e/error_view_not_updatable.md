# error_view_not_updatable

## Location
[src/backend/rewrite/rewriteHandler.c:3109-3203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L3109-L3203)

## Overview
Reports an error due to an attempt to update a non-updatable view, providing appropriate error messages and hints based on the specific command type.

## Definition
```c
void error_view_not_updatable(Relation view, CmdType command, List *mergeActionList, const char *detail)
```

## Detailed Description
This function is called when PostgreSQL detects an attempt to perform a DML operation (INSERT, UPDATE, DELETE, or MERGE) on a view that lacks the necessary INSTEAD OF triggers or DO INSTEAD rules to make it updatable. The function generates specific error messages tailored to each command type, providing users with actionable hints on how to make the view updatable.

The function handles both regular DML commands and MERGE operations differently. For MERGE, it iterates through all actions in the merge action list to check for missing triggers, since MERGE does not support rules and requires INSTEAD OF triggers for each action type.

This function is typically called from the rewriter during query planning, but can also be invoked by the executor as a just-in-case check via CheckValidResultRel().

## Parameters / Member Variables
- `view`: The Relation representing the view that cannot be updated
- `command`: The CmdType indicating the type of operation being attempted (INSERT, UPDATE, DELETE, or MERGE)
- `mergeActionList`: List of MergeAction nodes for MERGE commands (can be NULL for other commands)
- `detail`: Optional detailed error message explaining why the view is not updatable (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelationName
  - ereport
  - [errcode](errcode.md)
  - [errmsg](errmsg.md)
  - [errdetail_internal](errdetail_internal.md)
  - [errhint](errhint.md)
  - foreach_node
  - elog
- Called from (representative examples):
  - [CheckValidResultRel](../C/CheckValidResultRel.md) (src/backend/executor/execMain.c:1056)
  - [rewriteTargetView](../r/rewriteTargetView.md) (src/backend/rewrite/rewriteHandler.c:3275)
  - [RewriteQuery](../R/RewriteQuery.md) (src/backend/rewrite/rewriteHandler.c:4203)

## Notes and Other Information
- Uses PostgreSQL error reporting system with appropriate error codes (ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
- Provides different error hints for MERGE operations since they do not support rules, only INSTEAD OF triggers
- For MERGE commands, checks each action type individually and reports errors for actions lacking appropriate triggers
- The function never returns normally - it always throws an error using ereport() or elog()
- Error messages are internationalized using the _() macro for detail messages