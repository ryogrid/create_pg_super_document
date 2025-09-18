# markQueryForLocking

## Location
src/backend/rewrite/rewriteHandler.c: 1881 - 1944

## Overview
Recursively applies FOR UPDATE/SHARE locking clauses to all relations referenced in a query's join tree, propagating locking requirements through subqueries.

## Definition


## Detailed Description
This function traverses a query's join tree structure and applies FOR UPDATE or FOR SHARE locking clauses to all referenced relations. It implements recursive descent through different join tree node types and handles locking propagation with the following logic:

1. **Base relations (RTE_RELATION)**: Applies locking clause directly and sets ACL_SELECT_FOR_UPDATE permission requirement
2. **Subqueries (RTE_SUBQUERY)**: Applies locking to the subquery RTE and recursively propagates the locking to all relations within the subquery
3. **Join tree traversal**: Recursively processes FromExpr and JoinExpr nodes to reach all referenced relations
4. **Permission tracking**: Updates permission requirements to include SELECT FOR UPDATE privileges

The function ensures that locking semantics are consistently applied across complex query structures including views, subqueries, and joins.

## Parameters / Member Variables
- : The query whose relations should be marked for locking
- : The current join tree node being processed (RangeTblRef, FromExpr, or JoinExpr)
- : The lock strength (FOR UPDATE, FOR SHARE, etc.)
- : The lock wait policy (NOWAIT, SKIP LOCKED, etc.)
- : Boolean indicating if this locking was pushed down from an ancestor query level

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - [applyLockingClause](../a/applyLockingClause.md)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - nodeTag
  - [markQueryForLocking](markQueryForLocking.md) (recursive calls)
- Types used:
  - LockClauseStrength, LockWaitPolicy
  - RangeTblRef, FromExpr, JoinExpr
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md)
  - RTE_RELATION, RTE_SUBQUERY constants
- Called from:
  - [ApplyRetrieveRule](../A/ApplyRetrieveRule.md)
  - [markQueryForLocking](markQueryForLocking.md) (recursive calls)

## Notes and Other Information
- Must agree with the parser's transformLockingClause() routine for consistency
- May generate invalid queries (e.g., locking with aggregates) which are detected later by the planner
- Historically needed to avoid marking view OLD/NEW relations, though this logic may be simplifiable
- Recursive nature handles arbitrarily nested subqueries and complex join structures
- Permission requirements are updated to ensure proper access control for locked relations
- The pushedDown parameter tracks whether locking originated from higher query levels