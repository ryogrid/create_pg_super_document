# transformInsertStmt

## Location
[src/backend/parser/analyze.c:580-1007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L580-L1007)

## Overview
Transforms an INSERT statement from parse tree representation into a query tree structure that can be executed by the planner and executor.

## Definition
```c
static Query *
transformInsertStmt(ParseState *pstate, InsertStmt *stmt)
```

## Detailed Description
This function is the main entry point for transforming INSERT statements during the parse analysis phase. It handles multiple INSERT variants:

1. **INSERT ... DEFAULT VALUES** - Creates empty target list where all columns receive default values
2. **INSERT ... SELECT** - Transforms the SELECT subquery and builds target list from its output
3. **INSERT ... VALUES** (single row) - Directly transforms the VALUES list as the target list
4. **INSERT ... VALUES** (multiple rows) - Creates a VALUES RTE and references it with Vars

The function performs comprehensive processing including:
- WITH clause handling for CTEs
- Target table validation and permission checking
- Column list validation and default column generation
- Expression transformation and type coercion
- ON CONFLICT clause processing
- RETURNING clause processing

Key design considerations:
- Handles both simple VALUES and complex SELECT scenarios efficiently
- Maintains proper namespace isolation between main query and subqueries
- Ensures proper permission tracking for inserted columns
- Handles indirection (array/field assignments) correctly

## Parameters / Member Variables
- `pstate`: Parse state containing context information, namespace, and range table
- `stmt`: The parsed InsertStmt structure containing all INSERT clause information

## Dependencies
- Functions called/Symbols referenced:
  - [transformWithClause](transformWithClause.md) (for WITH/CTE processing)  
  - [setTargetTable](../s/setTargetTable.md) (for target table setup)
  - [checkInsertTargets](../c/checkInsertTargets.md) (for column validation)
  - [transformStmt](transformStmt.md) (for SELECT subquery processing)
  - [transformInsertRow](transformInsertRow.md) (for row expression processing)
  - [transformOnConflictClause](transformOnConflictClause.md) (for UPSERT handling)
  - [transformReturningList](transformReturningList.md) (for RETURNING clause)
  - [addRangeTableEntryForSubquery](../a/addRangeTableEntryForSubquery.md)/addRangeTableEntryForValues (RTE creation)
  - [assign_query_collations](../a/assign_query_collations.md) (collation assignment)

- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
- Sets pstate->p_is_insert = true to influence subsequent processing
- Handles the complex interaction between INSERT target columns and VALUES/SELECT sources
- Special handling for unknown-type constants to allow proper type coercion
- Supports both traditional INSERT and modern UPSERT (ON CONFLICT) functionality
- Manages range table entries carefully to support nested queries and CTEs
- Critical function in PostgreSQLs query transformation pipeline

## Simplified Source

```c
static Query *
transformInsertStmt(ParseState *pstate, InsertStmt *stmt)
{
    Query *qry = makeNode(Query);
    SelectStmt *selectStmt = (SelectStmt *) stmt->selectStmt;
    List *exprList = NIL;
    bool isGeneralSelect;
    List *icolumns;
    List *attrnos;
    bool isOnConflictUpdate;
    AclMode targetPerms;

    qry->commandType = CMD_INSERT;
    pstate->p_is_insert = true;

    // Process WITH clause
    if (stmt->withClause)
    {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    qry->override = stmt->override;
    isOnConflictUpdate = (stmt->onConflictClause &&
                         stmt->onConflictClause->action == ONCONFLICT_UPDATE);

    // Determine if this is a general SELECT vs simple VALUES
    isGeneralSelect = (selectStmt && (selectStmt->valuesLists == NIL ||
                                    selectStmt->sortClause != NIL ||
                                    selectStmt->limitOffset != NULL ||
                                    selectStmt->limitCount != NULL ||
                                    selectStmt->lockingClause != NIL ||
                                    selectStmt->withClause != NULL));

    // Set target permissions (INSERT + UPDATE if ON CONFLICT UPDATE)
    targetPerms = ACL_INSERT;
    if (isOnConflictUpdate)
        targetPerms |= ACL_UPDATE;

    // Set up target table
    qry->resultRelation = setTargetTable(pstate, stmt->relation, false, false, targetPerms);

    // Validate and process column list
    icolumns = checkInsertTargets(pstate, stmt->cols, &attrnos);

    // Handle different INSERT variants
    if (selectStmt == NULL)
    {
        // INSERT ... DEFAULT VALUES
        exprList = NIL;
    }
    else if (isGeneralSelect)
    {
        // INSERT ... SELECT
        ParseState *sub_pstate = make_parsestate(pstate);
        Query *selectQuery;

        // Transform the SELECT subquery
        selectQuery = transformStmt(sub_pstate, stmt->selectStmt);
        free_parsestate(sub_pstate);

        // Create RTE for the subquery
        ParseNamespaceItem *nsitem = addRangeTableEntryForSubquery(pstate,
                                                                  selectQuery,
                                                                  makeAlias("*SELECT*", NIL),
                                                                  false, false);
        addNSItemToQuery(pstate, nsitem, true, false, false);

        // Build expression list from subquery output
        exprList = NIL;
        foreach_node(TargetEntry, tle, selectQuery->targetList)
        {
            if (!tle->resjunk)
            {
                Expr *expr;
                if (tle->expr && (IsA(tle->expr, Const) || IsA(tle->expr, Param)) &&
                    exprType((Node *) tle->expr) == UNKNOWNOID)
                    expr = tle->expr;
                else
                {
                    Var *var = makeVarFromTargetEntry(nsitem->p_rtindex, tle);
                    var->location = exprLocation((Node *) tle->expr);
                    expr = (Expr *) var;
                }
                exprList = lappend(exprList, expr);
            }
        }

        exprList = transformInsertRow(pstate, exprList, stmt->cols, icolumns, attrnos, false);
    }
    else if (list_length(selectStmt->valuesLists) > 1)
    {
        // INSERT ... VALUES (multiple rows) - create VALUES RTE
        List *exprsLists = NIL;
        List *coltypes = NIL, *coltypmods = NIL, *colcollations = NIL;

        // Process each VALUES row
        foreach_node(List, sublist, selectStmt->valuesLists)
        {
            sublist = transformExpressionList(pstate, sublist, EXPR_KIND_VALUES, true);
            sublist = transformInsertRow(pstate, sublist, stmt->cols, icolumns, attrnos, true);
            assign_list_collations(pstate, sublist);
            exprsLists = lappend(exprsLists, sublist);
        }

        // Build column type info from first row
        foreach_node(Node, val, (List *) linitial(exprsLists))
        {
            coltypes = lappend_oid(coltypes, exprType(val));
            coltypmods = lappend_int(coltypmods, exprTypmod(val));
            colcollations = lappend_oid(colcollations, InvalidOid);
        }

        // Create VALUES RTE
        ParseNamespaceItem *nsitem = addRangeTableEntryForValues(pstate, exprsLists,
                                                                coltypes, coltypmods, colcollations,
                                                                NULL, false, true);
        addNSItemToQuery(pstate, nsitem, true, false, false);

        exprList = expandNSItemVars(pstate, nsitem, 0, -1, NULL);
        exprList = transformInsertRow(pstate, exprList, stmt->cols, icolumns, attrnos, false);
    }
    else
    {
        // INSERT ... VALUES (single row)
        List *valuesLists = selectStmt->valuesLists;
        exprList = transformExpressionList(pstate, (List *) linitial(valuesLists),
                                         EXPR_KIND_VALUES_SINGLE, true);
        exprList = transformInsertRow(pstate, exprList, stmt->cols, icolumns, attrnos, false);
    }

    // Build target list and mark permissions
    RTE ermissionInfo *perminfo = pstate->p_target_nsitem->p_perminfo;
    qry->targetList = NIL;
    forthree(lc, exprList, icols, icolumns, attnos, attrnos)
    {
        Expr *expr = (Expr *) lfirst(lc);
        ResTarget *col = lfirst_node(ResTarget, icols);
        AttrNumber attr_num = (AttrNumber) lfirst_int(attnos);
        TargetEntry *tle;

        tle = makeTargetEntry(expr, attr_num, col->name, false);
        qry->targetList = lappend(qry->targetList, tle);
        perminfo->insertedCols = bms_add_member(perminfo->insertedCols,
                                              attr_num - FirstLowInvalidHeapAttributeNumber);
    }

    // Process ON CONFLICT and RETURNING clauses
    if (stmt->onConflictClause || stmt->returningList)
    {
        pstate->p_namespace = NIL;
        addNSItemToQuery(pstate, pstate->p_target_nsitem, false, true, true);
    }

    if (stmt->onConflictClause)
        qry->onConflict = transformOnConflictClause(pstate, stmt->onConflictClause);

    if (stmt->returningList)
        qry->returningList = transformReturningList(pstate, stmt->returningList, EXPR_KIND_RETURNING);

    // Finalize query structure
    qry->rtable = pstate->p_rtable;
    qry->rteperminfos = pstate->p_rteperminfos;
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

    qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
    qry->hasSubLinks = pstate->p_hasSubLinks;

    assign_query_collations(pstate, qry);

    return qry;
}
```