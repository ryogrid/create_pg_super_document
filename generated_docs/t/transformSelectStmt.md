# transformSelectStmt

## Location
[src/backend/parser/analyze.c:1337-1479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1337-L1479)

## Overview
Transforms a SELECT statement AST node into a Query tree structure, handling all SELECT-specific clauses except set operations and VALUES lists.

## Definition

```c
static Query *
transformSelectStmt(ParseState *pstate, SelectStmt *stmt)
```
## Detailed Description
transformSelectStmt is the core function responsible for converting a parsed SELECT statement (SelectStmt) into PostgreSQL's internal Query representation. This function systematically processes each component of a SELECT statement in a specific order to ensure proper dependency resolution. It handles WITH clauses, FROM clauses, target lists, WHERE conditions, HAVING conditions, GROUP BY, ORDER BY, DISTINCT, LIMIT/OFFSET, window definitions, and locking clauses.

The function performs semantic analysis and validation while building the Query tree, including type resolution, column reference validation, and aggregate function checking. It maintains parse state throughout the transformation process to track context and resolve references between different parts of the query.

Note that this function specifically handles basic SELECT statements without set operations (UNION, INTERSECT, EXCEPT) or VALUES clauses, which are handled by separate transformation functions.

## Parameters / Member Variables
- `*pstate`: ParseState structure containing parsing context, symbol tables, and state information
- `*stmt`: SelectStmt node representing the parsed SELECT statement to be transformed
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (Query creation)
  - [transformWithClause](transformWithClause.md) (WITH clause processing)
  - [transformFromClause](transformFromClause.md) (FROM clause processing) 
  - [transformTargetList](transformTargetList.md) (SELECT target list processing)
  - [markTargetListOrigins](../m/markTargetListOrigins.md) (column origin tracking)
  - [transformWhereClause](transformWhereClause.md) (WHERE and HAVING clause processing)
  - [transformSortClause](transformSortClause.md) (ORDER BY processing)
  - [transformGroupClause](transformGroupClause.md) (GROUP BY processing)
  - [transformDistinctClause](transformDistinctClause.md)/transformDistinctOnClause (DISTINCT processing)
  - [transformLimitClause](transformLimitClause.md) (LIMIT/OFFSET processing)
  - [transformWindowDefinitions](transformWindowDefinitions.md) (window function processing)
  - [resolveTargetListUnknowns](../r/resolveTargetListUnknowns.md) (type resolution)
  - [makeFromExpr](../m/makeFromExpr.md) (join tree construction)
  - [transformLockingClause](transformLockingClause.md) (FOR UPDATE/SHARE processing)
  - [assign_query_collations](../a/assign_query_collations.md) (collation assignment)
  - [parseCheckAggregates](../p/parseCheckAggregates.md) (aggregate validation)
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
- The transformation order is critical: ORDER BY must be processed before GROUP BY and DISTINCT because they depend on the sort clause results
- The function can modify the target list during processing (passed by reference to transformation functions)
- Error handling includes validation for unsupported SELECT INTO syntax in contexts where it's not allowed
- Window definitions are processed after all window functions have been identified
- Aggregate validation is performed last after all other processing is complete
- The function sets various Query flags based on what constructs were found during parsing (hasSubLinks, hasWindowFuncs, etc.)

## Simplified Source

```c
static Query *
transformSelectStmt(ParseState *pstate, SelectStmt *stmt)
{
    Query *qry = makeNode(Query);
    Node *qual;
    ListCell *l;

    qry->commandType = CMD_SELECT;

    // Process WITH clause independently
    if (stmt->withClause)
    {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    // Reject SELECT INTO in contexts where it's not allowed
    if (stmt->intoClause)
        ereport(ERROR, "SELECT ... INTO is not allowed here");

    // Prepare for locking and window clauses
    pstate->p_locking_clause = stmt->lockingClause;
    pstate->p_windowdefs = stmt->windowClause;

    // Transform major clauses in dependency order
    transformFromClause(pstate, stmt->fromClause);
    qry->targetList = transformTargetList(pstate, stmt->targetList, EXPR_KIND_SELECT_TARGET);
    markTargetListOrigins(pstate, qry->targetList);

    qual = transformWhereClause(pstate, stmt->whereClause, EXPR_KIND_WHERE, "WHERE");
    qry->havingQual = transformWhereClause(pstate, stmt->havingClause, EXPR_KIND_HAVING, "HAVING");

    // Transform sorting/grouping (ORDER BY first for dependency reasons)
    qry->sortClause = transformSortClause(pstate, stmt->sortClause, &qry->targetList,
                                        EXPR_KIND_ORDER_BY, false);
    qry->groupClause = transformGroupClause(pstate, stmt->groupClause, &qry->groupingSets,
                                          &qry->targetList, qry->sortClause,
                                          EXPR_KIND_GROUP_BY, false);
    qry->groupDistinct = stmt->groupDistinct;

    // Handle DISTINCT variations
    if (stmt->distinctClause == NIL)
    {
        qry->distinctClause = NIL;
        qry->hasDistinctOn = false;
    }
    else if (linitial(stmt->distinctClause) == NULL)
    {
        // SELECT DISTINCT
        qry->distinctClause = transformDistinctClause(pstate, &qry->targetList,
                                                     qry->sortClause, false);
        qry->hasDistinctOn = false;
    }
    else
    {
        // SELECT DISTINCT ON
        qry->distinctClause = transformDistinctOnClause(pstate, stmt->distinctClause,
                                                       &qry->targetList, qry->sortClause);
        qry->hasDistinctOn = true;
    }

    // Transform LIMIT/OFFSET
    qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset,
                                          EXPR_KIND_OFFSET, "OFFSET", stmt->limitOption);
    qry->limitCount = transformLimitClause(pstate, stmt->limitCount,
                                         EXPR_KIND_LIMIT, "LIMIT", stmt->limitOption);
    qry->limitOption = stmt->limitOption;

    // Transform window definitions after finding all window functions
    qry->windowClause = transformWindowDefinitions(pstate, pstate->p_windowdefs, &qry->targetList);

    // Resolve unknown types as text
    if (pstate->p_resolve_unknowns)
        resolveTargetListUnknowns(pstate, qry->targetList);

    // Build final query structure
    qry->rtable = pstate->p_rtable;
    qry->rteperminfos = pstate->p_rteperminfos;
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

    // Set query flags from parse state
    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
    qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
    qry->hasAggs = pstate->p_hasAggs;

    // Process locking clauses
    foreach(l, stmt->lockingClause)
    {
        transformLockingClause(pstate, qry, (LockingClause *) lfirst(l), false);
    }

    assign_query_collations(pstate, qry);

    // Validate aggregates (must be after collation assignment)
    if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
        parseCheckAggregates(pstate, qry);

    return qry;
}
```