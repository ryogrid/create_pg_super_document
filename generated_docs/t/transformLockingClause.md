# transformLockingClause

## Location
[src/backend/parser/analyze.c:3302-3528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L3302-L3528)

## Overview
Transforms and validates FOR UPDATE/SHARE clauses during query analysis by converting relation names to relids and applying locking semantics to appropriate relations.

## Definition
```c
static void transformLockingClause(ParseState *pstate, Query *qry, LockingClause *lc, bool pushedDown)
```

## Detailed Description
This static function is the core implementation for processing row locking clauses in PostgreSQL. It performs two main operations: validation using CheckSelectLocking(), and transformation of the locking clause by replacing relation names with integer relation identifiers (relids).

The function handles two scenarios: when no specific relations are named (locks all applicable relations in the query), and when specific relations are named in the locking clause. For each applicable relation, it calls applyLockingClause() to record the locking requirement and updates permission requirements.

The function recursively processes subqueries to ensure locking clauses are properly propagated through the query tree. It also performs extensive validation to ensure locking clauses are not applied to incompatible relation types (joins, functions, VALUES clauses, etc.).

## Parameters / Member Variables
- `pstate`: Parser state containing context information for error reporting and name resolution
- `qry`: The Query structure being processed, which will be modified to include locking information  
- `lc`: The LockingClause structure from the parsed statement containing locking strength, wait policy, and target relations
- `pushedDown`: Boolean flag indicating whether this locking clause was pushed down from a parent query level

## Dependencies
- Functions called/Symbols referenced:
  - [CheckSelectLocking](../C/CheckSelectLocking.md) (validates locking compatibility)
  - [applyLockingClause](../a/applyLockingClause.md) (applies locking to specific relations)
  - [LCS_asString](../L/LCS_asString.md) (for error message formatting)
  - makeNode (creates new LockingClause nodes)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md) (retrieves permission info for relations)
  - ereport (error reporting)
  - Various RTE type constants (RTE_RELATION, RTE_SUBQUERY, etc.)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md) (for main SELECT statements)
  - [transformSetOperationStmt](transformSetOperationStmt.md) (for set operations)
  - [transformPLAssignStmt](transformPLAssignStmt.md) (for PL/pgSQL assignments)
  - [transformLockingClause](transformLockingClause.md) (recursively for subqueries)

## Notes and Other Information
- Static function not directly accessible outside analyze.c
- Recursively calls itself to handle subqueries with locking clauses
- Updates ACL_SELECT_FOR_UPDATE permission requirements for affected relations
- Creates an "allrels" clause for propagating locking to subqueries
- Uses inFromCl flag to exclude auto-added RTEs like NEW/OLD in rules
- Provides detailed error messages for unsupported locking targets with specific RTE type handling
- Cross-references with markQueryForLocking() in rewriteHandler.c and isLockedRefname() in parse_relation.c
- Located in src/backend/parser/analyze.c at lines 3302-3528

## Simplified Source

```c
static void
transformLockingClause(ParseState *pstate, Query *qry, LockingClause *lc,
                       bool pushedDown)
{
    List *lockedRels = lc->lockedRels;
    ListCell *l;
    ListCell *rt;
    Index i;
    LockingClause *allrels;

    CheckSelectLocking(qry, lc->strength);

    // Create clause to pass down to subqueries for all rels
    allrels = makeNode(LockingClause);
    allrels->lockedRels = NIL;  // indicates all rels
    allrels->strength = lc->strength;
    allrels->waitPolicy = lc->waitPolicy;

    if (lockedRels == NIL) {
        // Lock all regular tables used in query and subqueries
        i = 0;
        foreach(rt, qry->rtable) {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

            ++i;
            if (!rte->inFromCl)
                continue;

            switch (rte->rtekind) {
                case RTE_RELATION:
                    {
                        RTEPermissionInfo *perminfo;

                        applyLockingClause(qry, i, lc->strength,
                                           lc->waitPolicy, pushedDown);
                        perminfo = getRTEPermissionInfo(qry->rteperminfos, rte);
                        perminfo->requiredPerms |= ACL_SELECT_FOR_UPDATE;
                    }
                    break;
                case RTE_SUBQUERY:
                    applyLockingClause(qry, i, lc->strength, lc->waitPolicy,
                                       pushedDown);
                    // Propagate to subquery's rels
                    transformLockingClause(pstate, rte->subquery,
                                           allrels, true);
                    break;
                default:
                    // ignore JOIN, SPECIAL, FUNCTION, VALUES, CTE RTEs
                    break;
            }
        }
    }
    else {
        // Lock just the named tables
        foreach(l, lockedRels) {
            RangeVar *thisrel = (RangeVar *) lfirst(l);

            // Insist on unqualified alias names
            if (thisrel->catalogname || thisrel->schemaname)
                ereport(ERROR, /* qualified names not allowed */);

            i = 0;
            foreach(rt, qry->rtable) {
                RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);
                char *rtename = rte->eref->aliasname;

                ++i;
                if (!rte->inFromCl)
                    continue;

                // Handle aliasing rules for different RTE types
                if (rte->alias == NULL) {
                    if (rte->rtekind == RTE_JOIN) {
                        if (rte->join_using_alias == NULL)
                            continue;
                        rtename = rte->join_using_alias->aliasname;
                    }
                    else if (rte->rtekind == RTE_SUBQUERY ||
                             rte->rtekind == RTE_VALUES)
                        continue;
                }

                if (strcmp(rtename, thisrel->relname) == 0) {
                    switch (rte->rtekind) {
                        case RTE_RELATION:
                            {
                                RTEPermissionInfo *perminfo;

                                applyLockingClause(qry, i, lc->strength,
                                                   lc->waitPolicy, pushedDown);
                                perminfo = getRTEPermissionInfo(qry->rteperminfos, rte);
                                perminfo->requiredPerms |= ACL_SELECT_FOR_UPDATE;
                            }
                            break;
                        case RTE_SUBQUERY:
                            applyLockingClause(qry, i, lc->strength,
                                               lc->waitPolicy, pushedDown);
                            transformLockingClause(pstate, rte->subquery,
                                                   allrels, true);
                            break;
                        case RTE_JOIN:
                            ereport(ERROR, /* cannot be applied to a join */);
                            break;
                        case RTE_FUNCTION:
                            ereport(ERROR, /* cannot be applied to a function */);
                            break;
                        case RTE_TABLEFUNC:
                            ereport(ERROR, /* cannot be applied to a table function */);
                            break;
                        case RTE_VALUES:
                            ereport(ERROR, /* cannot be applied to VALUES */);
                            break;
                        case RTE_CTE:
                            ereport(ERROR, /* cannot be applied to a WITH query */);
                            break;
                        case RTE_NAMEDTUPLESTORE:
                            ereport(ERROR, /* cannot be applied to a named tuplestore */);
                            break;
                        default:
                            elog(ERROR, "unrecognized RTE type: %d",
                                 (int) rte->rtekind);
                            break;
                    }
                    break;  // out of foreach loop
                }
            }
            if (rt == NULL)
                ereport(ERROR, /* relation not found in FROM clause */);
        }
    }
}
```