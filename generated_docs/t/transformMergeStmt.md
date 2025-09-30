# transformMergeStmt

## Location
[src/backend/parser/parse_merge.c:107-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_merge.c#L107-L414)

## Overview
The main function that transforms a parsed MERGE statement AST into a Query tree structure, handling all aspects of MERGE statement analysis including permissions, namespace management, and action transformation.

## Definition
```c
Query *transformMergeStmt(ParseState *pstate, MergeStmt *stmt)
```

## Detailed Description
This function is the primary entry point for transforming MERGE statements during the parsing phase. It performs comprehensive analysis and transformation including:

1. **Permissions Analysis**: Collects required permissions (INSERT, UPDATE, DELETE, SELECT) based on the action types in WHEN clauses
2. **Validation**: Checks for unreachable WHEN clauses (those specified after unconditional ones) and validates relation types
3. **Namespace Setup**: Establishes proper visibility for target and source relations, handling namespace conflicts
4. **Join Condition Processing**: Transforms the ON condition that defines how source and target relations are matched
5. **Action Transformation**: Processes each WHEN clause, transforming their conditions and target lists according to their match type and command type
6. **Query Structure Creation**: Builds the complete Query structure with proper RTEs, join trees, and action lists

The function handles the three types of MERGE actions:
- **MATCHED**: Actions for rows that exist in both source and target
- **NOT MATCHED BY TARGET**: Actions for source rows with no target match (typically INSERT)
- **NOT MATCHED BY SOURCE**: Actions for target rows with no source match (typically UPDATE/DELETE)

## Parameters / Member Variables
- `pstate`: Parser state containing parsing context, namespace information, and accumulated state
- `stmt`: The parsed MERGE statement AST node containing all clause information

## Dependencies
- Functions called/Symbols referenced:
  - makeNode, setTargetTable, transformFromClause
  - [transformWithClause](transformWithClause.md), transformExpr, transformWhereClause
  - [transformReturningList](transformReturningList.md), transformUpdateTargetList, transformInsertRow
  - [checkInsertTargets](../c/checkInsertTargets.md), transformExpressionList
  - [setNamespaceForMergeWhen](../s/setNamespaceForMergeWhen.md), addNSItemToQuery
  - [GetNSItemByRangeTablePosn](../G/GetNSItemByRangeTablePosn.md), makeFromExpr, makeTargetEntry
  - [assign_query_collations](../a/assign_query_collations.md), errdetail_relkind_not_supported
  - Various ACL_* permission constants and CMD_* command type constants
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
- Validates that MERGE can only be performed on tables, partitioned tables, and views
- Handles WITH clauses but prohibits WITH RECURSIVE for MERGE statements
- Creates separate target lists for each action type (INSERT, UPDATE, DELETE, NOTHING)
- Manages complex namespace visibility rules where different actions can see different relations
- Supports INSERT DEFAULT VALUES syntax within MERGE statements
- Includes comprehensive permission checking to ensure all required privileges are validated
- The function creates a complete Query structure but leaves the actual join construction to later phases via transform_MERGE_to_join
- RETURNING clause processing is supported for MERGE statements
- Unreachable WHEN clause detection prevents logical errors in MERGE statement definitions

## Simplified Source

```c
Query *
transformMergeStmt(ParseState *pstate, MergeStmt *stmt)
{
    Query *qry = makeNode(Query);
    ListCell *l;
    AclMode targetPerms = ACL_NO_RIGHTS;
    bool is_terminal[NUM_MERGE_MATCH_KINDS];
    Index sourceRTI;
    List *mergeActionList;

    qry->commandType = CMD_MERGE;
    qry->hasRecursive = false;

    // Process WITH clause (but not WITH RECURSIVE)
    if (stmt->withClause)
    {
        if (stmt->withClause->recursive)
            ereport(ERROR, "WITH RECURSIVE is not supported for MERGE statement");

        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    // Analyze WHEN clauses for permissions and validate reachability
    is_terminal[MERGE_WHEN_MATCHED] = false;
    is_terminal[MERGE_WHEN_NOT_MATCHED_BY_SOURCE] = false;
    is_terminal[MERGE_WHEN_NOT_MATCHED_BY_TARGET] = false;

    foreach(l, stmt->mergeWhenClauses)
    {
        MergeWhenClause *mergeWhenClause = (MergeWhenClause *) lfirst(l);

        // Collect required permissions based on action type
        switch (mergeWhenClause->commandType)
        {
            case CMD_INSERT:
                targetPerms |= ACL_INSERT;
                break;
            case CMD_UPDATE:
                targetPerms |= ACL_UPDATE;
                break;
            case CMD_DELETE:
                targetPerms |= ACL_DELETE;
                break;
            case CMD_NOTHING:
                targetPerms |= ACL_SELECT;
                break;
        }

        // Check for unreachable WHEN clauses
        if (is_terminal[mergeWhenClause->matchKind])
            ereport(ERROR, "unreachable WHEN clause specified after unconditional WHEN clause");
        if (mergeWhenClause->condition == NULL)
            is_terminal[mergeWhenClause->matchKind] = true;
    }

    // Set up target table
    qry->resultRelation = setTargetTable(pstate, stmt->relation,
                                        stmt->relation->inh, false, targetPerms);
    qry->mergeTargetRelation = qry->resultRelation;

    // Validate target relation type
    if (pstate->p_target_relation->rd_rel->relkind != RELKIND_RELATION &&
        pstate->p_target_relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
        pstate->p_target_relation->rd_rel->relkind != RELKIND_VIEW)
        ereport(ERROR, "cannot execute MERGE on this relation type");

    // Transform source relation
    transformFromClause(pstate, list_make1(stmt->sourceRelation));
    sourceRTI = list_length(pstate->p_rtable);

    // Check for target/source name conflicts
    ParseNamespaceItem *nsitem = GetNSItemByRangeTablePosn(pstate, sourceRTI, 0);
    if (strcmp(pstate->p_target_nsitem->p_names->aliasname,
               nsitem->p_names->aliasname) == 0)
        ereport(ERROR, "name specified more than once in MERGE");

    qry->targetList = NIL;
    qry->rtable = pstate->p_rtable;
    qry->rteperminfos = pstate->p_rteperminfos;

    // Transform join condition
    addNSItemToQuery(pstate, pstate->p_target_nsitem, false, true, true);
    qry->mergeJoinCondition = transformExpr(pstate, stmt->joinCondition, EXPR_KIND_JOIN_ON);

    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    qry->returningList = transformReturningList(pstate, stmt->returningList, EXPR_KIND_MERGE_RETURNING);

    // Transform each WHEN clause into a MergeAction
    mergeActionList = NIL;
    foreach(l, stmt->mergeWhenClauses)
    {
        MergeWhenClause *mergeWhenClause = lfirst_node(MergeWhenClause, l);
        MergeAction *action = makeNode(MergeAction);

        action->commandType = mergeWhenClause->commandType;
        action->matchKind = mergeWhenClause->matchKind;

        // Set namespace for this action
        setNamespaceForMergeWhen(pstate, mergeWhenClause, qry->resultRelation, sourceRTI);

        // Transform WHEN condition
        action->qual = transformWhereClause(pstate, mergeWhenClause->condition,
                                          EXPR_KIND_MERGE_WHEN, "WHEN");

        // Transform target list based on action type
        switch (action->commandType)
        {
            case CMD_INSERT:
            {
                pstate->p_is_insert = true;
                List *icolumns = checkInsertTargets(pstate, mergeWhenClause->targetList, &attrnos);
                action->override = mergeWhenClause->override;

                if (mergeWhenClause->values == NIL)
                {
                    // INSERT ... DEFAULT VALUES
                    exprList = NIL;
                }
                else
                {
                    // Transform VALUES list
                    List *exprList = transformExpressionList(pstate, mergeWhenClause->values,
                                                            EXPR_KIND_VALUES_SINGLE, true);
                    exprList = transformInsertRow(pstate, exprList, mergeWhenClause->targetList,
                                                icolumns, attrnos, false);
                }

                // Build target list for INSERT
                RTE ermissionInfo *perminfo = pstate->p_target_nsitem->p_perminfo;
                forthree(lc, exprList, icols, icolumns, attnos, attrnos)
                {
                    Expr *expr = (Expr *) lfirst(lc);
                    ResTarget *col = lfirst_node(ResTarget, icols);
                    AttrNumber attr_num = (AttrNumber) lfirst_int(attnos);
                    TargetEntry *tle = makeTargetEntry(expr, attr_num, col->name, false);
                    action->targetList = lappend(action->targetList, tle);
                    perminfo->insertedCols = bms_add_member(perminfo->insertedCols,
                                                          attr_num - FirstLowInvalidHeapAttributeNumber);
                }
            }
            break;

            case CMD_UPDATE:
                pstate->p_is_insert = false;
                action->targetList = transformUpdateTargetList(pstate, mergeWhenClause->targetList);
                break;

            case CMD_DELETE:
                break;

            case CMD_NOTHING:
                action->targetList = NIL;
                break;
        }

        mergeActionList = lappend(mergeActionList, action);
    }

    qry->mergeActionList = mergeActionList;
    qry->hasTargetSRFs = false;
    qry->hasSubLinks = pstate->p_hasSubLinks;

    assign_query_collations(pstate, qry);

    return qry;
}
```