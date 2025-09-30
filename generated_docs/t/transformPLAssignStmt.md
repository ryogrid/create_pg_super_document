# transformPLAssignStmt

## Location
[src/backend/parser/analyze.c:2619-2867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L2619-L2867)

## Overview
Transforms a PL/pgSQL assignment statement into a SELECT query that computes the new value and handles type coercion and indirection operations.

## Definition

```c
struct a ColumnRef for the target variable.  If the target
	 * has more than one dotted name, we have to pull the extra names out of
	 * the indirection list.
	 */
	cref->fields = list_make1(makeString(stmt->name));
```
## Detailed Description
This function transforms a PL/pgSQL assignment statement into a Query structure representing a SELECT statement. The transformation handles both simple assignments and complex assignments involving field access or array subscripting through indirection. The function performs type checking and coercion using PL/pgSQL-specific coercion rules (COERCION_PLPGSQL) rather than standard SQL assignment coercion.

The transformation process involves:
1. Building a ColumnRef for the target variable, handling multi-part names
2. Transforming the target reference to get type information
3. Processing the SELECT statement that provides the assignment value
4. Performing type coercion between the source and target types
5. Handling indirection operations for field stores and array assignments
6. Processing standard SELECT clauses (WHERE, GROUP BY, ORDER BY, etc.)

## Parameters / Member Variables
- : Parse state containing context information for the transformation
- : The PL/pgSQL assignment statement to transform, containing:
  - : Target variable name
  - : Number of dotted names in the target
  - : List of field/array access operations
  - : SelectStmt providing the assignment value
  - : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode, makeString, list_make1, list_copy, list_delete_first
  - [transformExpr](transformExpr.md), transformFromClause, transformTargetList
  - [transformAssignmentIndirection](transformAssignmentIndirection.md), coerce_to_target_type
  - [transformWhereClause](transformWhereClause.md), transformSortClause, transformGroupClause
  - [transformDistinctClause](transformDistinctClause.md), transformDistinctOnClause
  - [transformLimitClause](transformLimitClause.md), transformWindowDefinitions
  - [transformLockingClause](transformLockingClause.md), assign_query_collations, parseCheckAggregates
  - [exprType](../e/exprType.md), exprTypmod, exprCollation, exprLocation
  - [format_type_be](../f/format_type_be.md), makeFromExpr
- Called from (representative examples):
  - [transformStmt](transformStmt.md)

## Notes and Other Information
- Uses COERCION_PLPGSQL instead of COERCION_ASSIGNMENT for type coercion
- Handles composite types specially to maintain backwards compatibility
- The function expects exactly one item in the SELECT target list
- Supports complex assignments through indirection (field access, array subscripting)
- Processes standard SELECT features like WHERE, GROUP BY, ORDER BY, LIMIT, DISTINCT, and window functions
- Performs aggregate function validation if aggregates are present in the query

## Simplified Source

```c
static Query *
transformPLAssignStmt(ParseState *pstate, PLAssignStmt *stmt)
{
    Query *qry = makeNode(Query);
    ColumnRef *cref = makeNode(ColumnRef);
    List *indirection = stmt->indirection;
    int nnames = stmt->nnames;
    SelectStmt *sstmt = stmt->val;
    Node *target;
    Oid targettype, targetcollation;
    int32 targettypmod;
    List *tlist;
    TargetEntry *tle;
    Oid type_id;
    Node *qual;

    // Build ColumnRef for target variable
    cref->fields = list_make1(makeString(stmt->name));
    cref->location = stmt->location;

    // Handle multi-part target names
    if (nnames > 1)
    {
        indirection = list_copy(indirection);
        while (--nnames > 0 && indirection != NIL)
        {
            Node *ind = (Node *) linitial(indirection);
            if (!IsA(ind, String))
                elog(ERROR, "invalid name count in PLAssignStmt");
            cref->fields = lappend(cref->fields, ind);
            indirection = list_delete_first(indirection);
        }
    }

    // Transform target reference to get type information
    target = transformExpr(pstate, (Node *) cref, EXPR_KIND_UPDATE_TARGET);
    targettype = exprType(target);
    targettypmod = exprTypmod(target);
    targetcollation = exprCollation(target);

    qry->commandType = CMD_SELECT;
    pstate->p_is_insert = false;

    // Set up locking and window info
    pstate->p_locking_clause = sstmt->lockingClause;
    pstate->p_windowdefs = sstmt->windowClause;

    // Process FROM clause and target list
    transformFromClause(pstate, sstmt->fromClause);
    tlist = transformTargetList(pstate, sstmt->targetList, EXPR_KIND_SELECT_TARGET);

    // Verify exactly one target list item
    if (list_length(tlist) != 1)
        ereport(ERROR, "assignment source returned %d columns", list_length(tlist));

    tle = linitial_node(TargetEntry, tlist);
    type_id = exprType((Node *) tle->expr);
    pstate->p_expr_kind = EXPR_KIND_UPDATE_TARGET;

    // Handle indirection (field/array access) or direct assignment
    if (indirection)
    {
        tle->expr = (Expr *) transformAssignmentIndirection(pstate, target, stmt->name, false,
                                                           targettype, targettypmod, targetcollation,
                                                           indirection, list_head(indirection),
                                                           (Node *) tle->expr, COERCION_PLPGSQL,
                                                           exprLocation(target));
    }
    else if (targettype != type_id &&
             (targettype == RECORDOID || ISCOMPLEX(targettype)) &&
             (type_id == RECORDOID || ISCOMPLEX(type_id)))
    {
        // Special handling for composite types (backwards compatibility)
    }
    else
    {
        // Normal type coercion using PL/pgSQL rules
        Node *orig_expr = (Node *) tle->expr;
        tle->expr = (Expr *) coerce_to_target_type(pstate, orig_expr, type_id,
                                                  targettype, targettypmod,
                                                  COERCION_PLPGSQL, COERCE_IMPLICIT_CAST, -1);
        if (tle->expr == NULL)
            ereport(ERROR, "variable type mismatch in assignment");
    }

    pstate->p_expr_kind = EXPR_KIND_NONE;
    qry->targetList = list_make1(tle);

    // Transform standard SELECT clauses
    qual = transformWhereClause(pstate, sstmt->whereClause, EXPR_KIND_WHERE, "WHERE");
    qry->havingQual = transformWhereClause(pstate, sstmt->havingClause, EXPR_KIND_HAVING, "HAVING");

    // Transform sorting/grouping (ORDER BY first)
    qry->sortClause = transformSortClause(pstate, sstmt->sortClause, &qry->targetList,
                                        EXPR_KIND_ORDER_BY, false);
    qry->groupClause = transformGroupClause(pstate, sstmt->groupClause, &qry->groupingSets,
                                          &qry->targetList, qry->sortClause,
                                          EXPR_KIND_GROUP_BY, false);

    // Handle DISTINCT
    if (sstmt->distinctClause == NIL)
    {
        qry->distinctClause = NIL;
        qry->hasDistinctOn = false;
    }
    else if (linitial(sstmt->distinctClause) == NULL)
    {
        qry->distinctClause = transformDistinctClause(pstate, &qry->targetList,
                                                     qry->sortClause, false);
        qry->hasDistinctOn = false;
    }
    else
    {
        qry->distinctClause = transformDistinctOnClause(pstate, sstmt->distinctClause,
                                                       &qry->targetList, qry->sortClause);
        qry->hasDistinctOn = true;
    }

    // Transform LIMIT and window clauses
    qry->limitOffset = transformLimitClause(pstate, sstmt->limitOffset,
                                          EXPR_KIND_OFFSET, "OFFSET", sstmt->limitOption);
    qry->limitCount = transformLimitClause(pstate, sstmt->limitCount,
                                         EXPR_KIND_LIMIT, "LIMIT", sstmt->limitOption);
    qry->limitOption = sstmt->limitOption;

    qry->windowClause = transformWindowDefinitions(pstate, pstate->p_windowdefs, &qry->targetList);

    // Finalize query structure
    qry->rtable = pstate->p_rtable;
    qry->rteperminfos = pstate->p_rteperminfos;
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
    qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
    qry->hasAggs = pstate->p_hasAggs;

    // Process locking clauses
    foreach_node(LockingClause, lc, sstmt->lockingClause)
    {
        transformLockingClause(pstate, qry, lc, false);
    }

    assign_query_collations(pstate, qry);

    // Validate aggregates if present
    if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
        parseCheckAggregates(pstate, qry);

    return qry;
}
```