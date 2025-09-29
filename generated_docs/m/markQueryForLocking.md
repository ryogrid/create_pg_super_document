# markQueryForLocking

## Location
[src/backend/rewrite/rewriteHandler.c:1881-1944](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1881-L1944)

## Overview
Recursively applies FOR UPDATE/SHARE locking clauses to all relations referenced in a query's join tree, propagating locking requirements through subqueries.

## Definition

```c
static void
markQueryForLocking(Query *qry, Node *jtnode,
					LockClauseStrength strength, LockWaitPolicy waitPolicy,
					bool pushedDown)
```
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
  - [RangeTblRef](../R/RangeTblRef.md), FromExpr, JoinExpr
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

## Simplified Source

```c
static void markQueryForLocking(Query *qry, Node *jtnode,
                               LockClauseStrength strength, LockWaitPolicy waitPolicy,
                               bool pushedDown) {
    if (jtnode == NULL)
        return;

    if (IsA(jtnode, RangeTblRef)) {
        // Handle range table reference
        int rti = ((RangeTblRef *) jtnode)->rtindex;
        RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

        if (rte->rtekind == RTE_RELATION) {
            // Base relation: apply locking and update permissions
            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown);

            RTEPermissionInfo *perminfo = getRTEPermissionInfo(qry->rteperminfos, rte);
            perminfo->requiredPerms |= ACL_SELECT_FOR_UPDATE;

        } else if (rte->rtekind == RTE_SUBQUERY) {
            // Subquery: apply locking and propagate to subquery relations
            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown);

            // Recursively mark subquery relations
            markQueryForLocking(rte->subquery, (Node *) rte->subquery->jointree,
                              strength, waitPolicy, true);
        }
        // Other RTE types (functions, values, etc.) are unaffected

    } else if (IsA(jtnode, FromExpr)) {
        // FROM clause: process all items in the from list
        FromExpr *f = (FromExpr *) jtnode;

        foreach(l, f->fromlist) {
            markQueryForLocking(qry, lfirst(l), strength, waitPolicy, pushedDown);
        }

    } else if (IsA(jtnode, JoinExpr)) {
        // JOIN expression: process both left and right sides
        JoinExpr *j = (JoinExpr *) jtnode;

        markQueryForLocking(qry, j->larg, strength, waitPolicy, pushedDown);
        markQueryForLocking(qry, j->rarg, strength, waitPolicy, pushedDown);

    } else {
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(jtnode));
    }
}
```