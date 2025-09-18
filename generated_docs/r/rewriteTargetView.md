# rewriteTargetView

## Location
[src/backend/rewrite/rewriteHandler.c:3204-3864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L3204-L3864)

## Overview
Attempts to rewrite a query where the target relation is a view, transforming it so that the view's base relation becomes the target relation.

## Definition
```c
static Query *rewriteTargetView(Query *parsetree, Relation view)
```

## Detailed Description
This function is responsible for the complex process of rewriting DML operations (INSERT, UPDATE, DELETE, MERGE) that target views to instead target the underlying base relations. This transformation is a key part of PostgreSQL's view updatability mechanism for automatically updatable views.

The function performs extensive validation to ensure the view is updatable, handles permission checking with proper user context (view owner vs. query caller based on security_invoker), adjusts column references and permissions, and handles special cases like ON CONFLICT clauses and WITH CHECK OPTION constraints.

Key operations include:
- Validating view updatability using view_query_is_auto_updatable()
- Checking that modified columns are updatable
- Creating new RTEs for the base relation with proper locking
- Adjusting variable references and permission sets
- Handling security barriers and WITH CHECK OPTION constraints
- Managing MERGE command specifics and INSTEAD OF trigger interactions

## Parameters / Member Variables
- `parsetree`: The Query node representing the DML statement targeting the view
- `view`: The Relation representing the view being targeted for modification

## Dependencies
- Functions called/Symbols referenced:
  - [get_view_query](../g/get_view_query.md)
  - copyObject
  - rt_fetch
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - [view_query_is_auto_updatable](../v/view_query_is_auto_updatable.md)
  - [error_view_not_updatable](../e/error_view_not_updatable.md)
  - [view_cols_are_auto_updatable](../v/view_cols_are_auto_updatable.md)
  - [view_has_instead_trigger](../v/view_has_instead_trigger.md)
  - table_open
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)
  - [addRTEPermissionInfo](../a/addRTEPermissionInfo.md)
  - [adjust_view_column_set](../a/adjust_view_column_set.md)
  - RelationHasSecurityInvoker
  - RelationIsSecurityView
  - RelationHasCheckOption
  - [AddQual](../A/AddQual.md)
- Called from (representative examples):
  - [RewriteQuery](../R/RewriteQuery.md) (src/backend/rewrite/rewriteHandler.c:4213)

## Notes and Other Information
- Requires RowExclusiveLock on the base relation since it becomes the target
- Handles security invoker vs definer semantics for permission checking
- For MERGE commands, validates that there are no partial INSTEAD OF triggers (either all actions must have triggers or none)
- Preserves security barrier semantics by moving security quals to the new target RTE
- Updates column permission bitmaps to reflect the transformation from view columns to base relation columns
- Handles special ON CONFLICT processing by creating new EXCLUDED pseudo-relations
- Implements WITH CHECK OPTION inheritance including cascaded check options
- The function assumes the view contains exactly one base relation (validated by view_query_is_auto_updatable)
- Error handling includes specific messages for different command types and non-updatable column scenarios