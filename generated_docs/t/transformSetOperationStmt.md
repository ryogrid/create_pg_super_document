# transformSetOperationStmt

## Location
[src/backend/parser/analyze.c:1699-1955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1699-L1955)

## Overview
Transforms a set-operations tree (UNION/INTERSECT/EXCEPT) into a Query containing the leaf SELECTs as subqueries and a top-level setOperations tree.

## Definition

```c
static Query *
transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt)
```
## Detailed Description
transformSetOperationStmt handles the transformation of complex SELECT statements that involve set operations (UNION, INTERSECT, EXCEPT). The function builds a top-level Query structure that contains the individual SELECT statements as subqueries in its range table, with the set operation tree stored in the Query's setOperations field.

The transformation process involves several critical steps: First, it validates that no INTO clauses exist in inappropriate contexts and extracts top-level clauses (ORDER BY, LIMIT, locking) that must be handled at the outer level rather than recursively. It then calls transformSetOperationTree to recursively process the set operation tree structure.

After the tree transformation, the function constructs a dummy target list for the outer query using column names from the leftmost SELECT and common data types/collations determined by the set operations. This target list allows ORDER BY clauses to reference result columns by name or position. The function also creates a temporary namespace entry to enable proper column reference resolution in ORDER BY expressions.

A key restriction is that ORDER BY clauses can only reference result columns by name or number (SQL92-style), not arbitrary expressions, which is enforced by checking that no new target list entries are added during ORDER BY processing.

## Parameters / Member Variables
- `*pstate`: ParseState structure containing parsing context and range table information
- `*stmt`: SelectStmt node representing the set operation tree to be transformed
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (Query creation)
  - [transformWithClause](transformWithClause.md) (WITH clause processing)
  - [transformSetOperationTree](transformSetOperationTree.md) (recursive set operation tree processing)
  - rt_fetch (range table entry retrieval)
  - [makeVar](../m/makeVar.md)/makeTargetEntry (dummy target list construction)
  - [addRangeTableEntryForJoin](../a/addRangeTableEntryForJoin.md) (temporary namespace creation for ORDER BY)
  - [transformSortClause](transformSortClause.md) (ORDER BY processing)
  - [transformLimitClause](transformLimitClause.md) (LIMIT/OFFSET processing)
  - [assign_query_collations](../a/assign_query_collations.md) (collation assignment)
  - [parseCheckAggregates](../p/parseCheckAggregates.md) (aggregate validation)
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
- The function rejects FOR UPDATE/SHARE clauses with set operations as they are not currently supported
- INTO clauses are only allowed in the leftmost SELECT of a set operation tree
- The leftmost SELECT's column names are used for the result columns, while data types and collations come from the common types determined across all set operations
- A temporary Join RTE is created during ORDER BY processing to provide proper namespace resolution for result columns
- The function enforces SQL92 restrictions on ORDER BY clauses - only result column names/numbers are allowed, not expressions
- Memory management includes proper cleanup of temporary namespace entries and range table modifications
- The transformation preserves the original set operation tree structure in the Query's setOperations field for later processing by the planner

## Simplified Source

```c
static Query *
transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt)
{
    Query *qry = makeNode(Query);
    SelectStmt *leftmostSelect;
    int leftmostRTI;
    Query *leftmostQuery;
    SetOperationStmt *sostmt;
    List *sortClause, *lockingClause;
    Node *limitOffset, *limitCount;
    WithClause *withClause;
    List *targetvars, *targetnames;
    ParseNamespaceItem *jnsitem;
    ParseNamespaceColumn *sortnscolumns;
    int sortcolindex, tllen;

    qry->commandType = CMD_SELECT;

    // Find leftmost leaf SELECT to check for illegal INTO clause
    leftmostSelect = stmt->larg;
    while (leftmostSelect && leftmostSelect->op != SETOP_NONE)
        leftmostSelect = leftmostSelect->larg;

    if (leftmostSelect->intoClause)
        ereport(ERROR, "SELECT ... INTO is not allowed here");

    // Extract top-level clauses before recursive processing
    sortClause = stmt->sortClause;
    limitOffset = stmt->limitOffset;
    limitCount = stmt->limitCount;
    lockingClause = stmt->lockingClause;
    withClause = stmt->withClause;

    // Clear these from stmt to prevent recursive handling
    stmt->sortClause = NIL;
    stmt->limitOffset = NULL;
    stmt->limitCount = NULL;
    stmt->lockingClause = NIL;
    stmt->withClause = NULL;

    // Reject FOR UPDATE/SHARE with set operations
    if (lockingClause)
        ereport(ERROR, "%s is not allowed with UNION/INTERSECT/EXCEPT",
                LCS_asString(((LockingClause *) linitial(lockingClause))->strength));

    // Process WITH clause
    if (withClause)
    {
        qry->hasRecursive = withClause->recursive;
        qry->cteList = transformWithClause(pstate, withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    // Recursively transform the set operation tree
    sostmt = castNode(SetOperationStmt, transformSetOperationTree(pstate, stmt, true, NULL));
    qry->setOperations = (Node *) sostmt;

    // Find leftmost SELECT in transformed tree
    Node *node = sostmt->larg;
    while (node && IsA(node, SetOperationStmt))
        node = ((SetOperationStmt *) node)->larg;

    leftmostRTI = ((RangeTblRef *) node)->rtindex;
    leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;

    // Build dummy target list using leftmost column names and common types
    qry->targetList = NIL;
    targetvars = NIL;
    targetnames = NIL;
    sortnscolumns = (ParseNamespaceColumn *)
        palloc0(list_length(sostmt->colTypes) * sizeof(ParseNamespaceColumn));
    sortcolindex = 0;

    forfour(lct, sostmt->colTypes,
            lcm, sostmt->colTypmods,
            lcc, sostmt->colCollations,
            left_tlist, leftmostQuery->targetList)
    {
        Oid colType = lfirst_oid(lct);
        int32 colTypmod = lfirst_int(lcm);
        Oid colCollation = lfirst_oid(lcc);
        TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
        char *colName = pstrdup(lefttle->resname);

        Var *var = makeVar(leftmostRTI, lefttle->resno, colType, colTypmod, colCollation, 0);
        var->location = exprLocation((Node *) lefttle->expr);

        TargetEntry *tle = makeTargetEntry((Expr *) var, (AttrNumber) pstate->p_next_resno++,
                                          colName, false);
        qry->targetList = lappend(qry->targetList, tle);
        targetvars = lappend(targetvars, var);
        targetnames = lappend(targetnames, makeString(colName));

        // Set up namespace column info for ORDER BY
        sortnscolumns[sortcolindex].p_varno = leftmostRTI;
        sortnscolumns[sortcolindex].p_varattno = lefttle->resno;
        sortnscolumns[sortcolindex].p_vartype = colType;
        sortnscolumns[sortcolindex].p_vartypmod = colTypmod;
        sortnscolumns[sortcolindex].p_varcollid = colCollation;
        sortnscolumns[sortcolindex].p_varnosyn = leftmostRTI;
        sortnscolumns[sortcolindex].p_varattnosyn = lefttle->resno;
        sortcolindex++;
    }

    // Create temporary namespace for ORDER BY processing
    int sv_rtable_length = list_length(pstate->p_rtable);
    jnsitem = addRangeTableEntryForJoin(pstate, targetnames, sortnscolumns,
                                       JOIN_INNER, 0, targetvars, NIL, NIL,
                                       NULL, NULL, false);
    List *sv_namespace = pstate->p_namespace;
    pstate->p_namespace = NIL;
    addNSItemToQuery(pstate, jnsitem, false, false, true);

    // Transform ORDER BY (restricted to result column names/numbers only)
    tllen = list_length(qry->targetList);
    qry->sortClause = transformSortClause(pstate, sortClause, &qry->targetList,
                                        EXPR_KIND_ORDER_BY, false);

    // Restore namespace and check for illegal expressions in ORDER BY
    pstate->p_namespace = sv_namespace;
    pstate->p_rtable = list_truncate(pstate->p_rtable, sv_rtable_length);

    if (tllen != list_length(qry->targetList))
        ereport(ERROR, "invalid UNION/INTERSECT/EXCEPT ORDER BY clause");

    // Transform LIMIT/OFFSET
    qry->limitOffset = transformLimitClause(pstate, limitOffset,
                                          EXPR_KIND_OFFSET, "OFFSET", stmt->limitOption);
    qry->limitCount = transformLimitClause(pstate, limitCount,
                                         EXPR_KIND_LIMIT, "LIMIT", stmt->limitOption);
    qry->limitOption = stmt->limitOption;

    // Finalize query structure
    qry->rtable = pstate->p_rtable;
    qry->rteperminfos = pstate->p_rteperminfos;
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
    qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
    qry->hasAggs = pstate->p_hasAggs;

    assign_query_collations(pstate, qry);

    // Validate aggregates if present
    if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
        parseCheckAggregates(pstate, qry);

    return qry;
}
```