# ExecInitModifyTable

## Location
[src/backend/executor/nodeModifyTable.c:4422-4822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L4422-L4822)

## Overview
Initializes the execution state for a ModifyTable plan node, setting up all necessary structures for DML operations (INSERT, UPDATE, DELETE, MERGE) including result relations, triggers, constraints, and partitioning.

## Definition

```c
structure
	 */
	mtstate = makeNode(ModifyTableState);
```
## Detailed Description
This comprehensive initialization function sets up a ModifyTableState structure for executing DML operations. It handles complex scenarios including partitioned tables, foreign tables, inheritance hierarchies, ON CONFLICT clauses, RETURNING projections, WITH CHECK OPTIONS, and MERGE operations.

The function performs several key initialization phases:
1. Creates and initializes the ModifyTableState structure
2. Resolves the root target relation and sets up partition routing if needed
3. Initializes all result relations in the ResultRelInfo array
4. Sets up EPQ (EvalPlanQual) state for concurrent tuple visibility
5. Initializes the subplan that provides input tuples
6. Configures junk attributes for row identification (ctid/wholerow)
7. Sets up RETURNING projections, ON CONFLICT handling, and WITH CHECK OPTIONS
8. Prepares foreign data wrapper interfaces
9. Builds hash tables for efficient result relation lookup when many relations are involved

## Parameters / Member Variables
- : ModifyTable plan node containing the DML operation details and configuration
- : Execution state providing transaction context and global execution information
- : Execution flags controlling behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY for explain-only mode)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitResultRelation](ExecInitResultRelation.md)
  - [ExecInitNode](ExecInitNode.md)
  - [EvalPlanQualInit](EvalPlanQualInit.md)
  - [EvalPlanQualSetPlan](EvalPlanQualSetPlan.md)
  - [ExecSetupTransitionCaptureState](ExecSetupTransitionCaptureState.md)
  - [ExecSetupPartitionTupleRouting](ExecSetupPartitionTupleRouting.md)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
  - [ExecInitQual](ExecInitQual.md)
  - [ExecFindJunkAttributeInTlist](ExecFindJunkAttributeInTlist.md)
  - [CheckValidResultRel](../C/CheckValidResultRel.md)
  - [ExecInitMerge](ExecInitMerge.md)
- Data structures used:
  - [ModifyTableState](../M/ModifyTableState.md)
  - [ResultRelInfo](../R/ResultRelInfo.md)
  - [OnConflictSetState](../O/OnConflictSetState.md)
  - [PlanRowMark](../P/PlanRowMark.md)
  - [ExecRowMark](ExecRowMark.md)
  - [ExecAuxRowMark](ExecAuxRowMark.md)
- Called from:
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Supports all DML operations: INSERT, UPDATE, DELETE, and MERGE
- Handles complex partitioning scenarios with tuple routing for INSERT operations
- Sets up hash tables (mt_resultOidHash) for efficient relation lookup when dealing with many target relations (threshold typically 64 relations)
- Properly initializes foreign data wrapper hooks for foreign table modifications
- Manages row identification through ctid (for regular tables) or wholerow (for foreign tables and views)
- Supports transition table capture for statement-level triggers
- Handles ON CONFLICT DO UPDATE with complete projection setup
- Integrates with EvalPlanQual for proper handling of concurrent updates
- Must be paired with ExecEndModifyTable for proper cleanup
- Critical entry point for all table modification operations in PostgreSQL's executor

## Simplified Source

```c
ModifyTableState *
ExecInitModifyTable(ModifyTable *node, EState *estate, int eflags)
{
    ModifyTableState *mtstate;
    Plan *subplan = outerPlan(node);
    CmdType operation = node->operation;
    int nrels = list_length(node->resultRelations);
    ResultRelInfo *resultRelInfo;
    int i;
    Relation rel;

    // Create and initialize the main state structure
    mtstate = makeNode(ModifyTableState);
    mtstate->ps.plan = (Plan *) node;
    mtstate->ps.state = estate;
    mtstate->ps.ExecProcNode = ExecModifyTable;

    mtstate->operation = operation;
    mtstate->canSetTag = node->canSetTag;
    mtstate->mt_done = false;
    mtstate->mt_nrels = nrels;

    // Allocate array for result relation info
    mtstate->resultRelInfo = (ResultRelInfo *)
        palloc(nrels * sizeof(ResultRelInfo));

    // Initialize MERGE operation counters
    mtstate->mt_merge_pending_not_matched = NULL;
    mtstate->mt_merge_inserted = 0;
    mtstate->mt_merge_updated = 0;
    mtstate->mt_merge_deleted = 0;

    // Set up root result relation (target table)
    if (node->rootRelation > 0) {
        // Partitioned/inherited table case
        mtstate->rootResultRelInfo = makeNode(ResultRelInfo);
        ExecInitResultRelation(estate, mtstate->rootResultRelInfo,
                             node->rootRelation);
    } else {
        // Single relation case
        mtstate->rootResultRelInfo = mtstate->resultRelInfo;
        ExecInitResultRelation(estate, mtstate->resultRelInfo,
                             linitial_int(node->resultRelations));
    }

    // Initialize EPQ state for concurrent tuple handling
    EvalPlanQualInit(&mtstate->mt_epqstate, estate, NULL, NIL,
                     node->epqParam, node->resultRelations);
    mtstate->fireBSTriggers = true;

    // Setup transition tuple capture if not in explain-only mode
    if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
        ExecSetupTransitionCaptureState(mtstate, estate);

    // Initialize all result relations
    resultRelInfo = mtstate->resultRelInfo;
    i = 0;
    foreach(l, node->resultRelations) {
        Index resultRelation = lfirst_int(l);
        List *mergeActions = NIL;

        if (node->mergeActionLists)
            mergeActions = list_nth(node->mergeActionLists, i);

        if (resultRelInfo != mtstate->rootResultRelInfo) {
            ExecInitResultRelation(estate, resultRelInfo, resultRelation);
            resultRelInfo->ri_RootResultRelInfo = mtstate->rootResultRelInfo;
        }

        // Set FDW direct modify flag
        resultRelInfo->ri_usesFdwDirectModify =
            bms_is_member(i, node->fdwDirectModifyPlans);

        // Validate relation for the operation
        CheckValidResultRel(resultRelInfo, operation, mergeActions);

        resultRelInfo++;
        i++;
    }

    // Initialize the subplan that provides input tuples
    outerPlanState(mtstate) = ExecInitNode(subplan, estate, eflags);

    // Set up per-relation specifics (FDW, junk attributes)
    for (i = 0; i < nrels; i++) {
        resultRelInfo = &mtstate->resultRelInfo[i];

        // Initialize FDW for foreign tables
        if (!resultRelInfo->ri_usesFdwDirectModify &&
            resultRelInfo->ri_FdwRoutine != NULL &&
            resultRelInfo->ri_FdwRoutine->BeginForeignModify != NULL) {
            List *fdw_private = (List *) list_nth(node->fdwPrivLists, i);
            resultRelInfo->ri_FdwRoutine->BeginForeignModify(mtstate,
                                                           resultRelInfo,
                                                           fdw_private, i, eflags);
        }

        // Find junk attributes for row identification (UPDATE/DELETE/MERGE)
        if (operation == CMD_UPDATE || operation == CMD_DELETE || operation == CMD_MERGE) {
            char relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;

            if (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW ||
                relkind == RELKIND_PARTITIONED_TABLE) {
                // Use ctid for regular tables
                resultRelInfo->ri_RowIdAttNo =
                    ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
            } else {
                // Use wholerow for foreign tables and others
                resultRelInfo->ri_RowIdAttNo =
                    ExecFindJunkAttributeInTlist(subplan->targetlist, "wholerow");
            }
        }
    }

    // Set up tableoid junk attribute for inherited operations
    mtstate->mt_resultOidAttno =
        ExecFindJunkAttributeInTlist(subplan->targetlist, "tableoid");
    mtstate->mt_lastResultOid = InvalidOid;
    mtstate->mt_lastResultIndex = 0;

    // Get root target relation
    rel = mtstate->rootResultRelInfo->ri_RelationDesc;

    // Set up partition tuple routing for INSERT into partitioned tables
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
        operation == CMD_INSERT)
        mtstate->mt_partition_tuple_routing =
            ExecSetupPartitionTupleRouting(estate, rel);

    // Initialize WITH CHECK OPTION constraints
    resultRelInfo = mtstate->resultRelInfo;
    foreach(l, node->withCheckOptionLists) {
        List *wcoList = (List *) lfirst(l);
        List *wcoExprs = NIL;

        foreach(ll, wcoList) {
            WithCheckOption *wco = (WithCheckOption *) lfirst(ll);
            ExprState *wcoExpr = ExecInitQual((List *) wco->qual, &mtstate->ps);
            wcoExprs = lappend(wcoExprs, wcoExpr);
        }

        resultRelInfo->ri_WithCheckOptions = wcoList;
        resultRelInfo->ri_WithCheckOptionExprs = wcoExprs;
        resultRelInfo++;
    }

    // Initialize RETURNING projections if present
    if (node->returningLists) {
        // Set up result tuple slot and expression context
        mtstate->ps.plan->targetlist = (List *) linitial(node->returningLists);
        ExecInitResultTupleSlotTL(&mtstate->ps, &TTSOpsVirtual);

        if (mtstate->ps.ps_ExprContext == NULL)
            ExecAssignExprContext(estate, &mtstate->ps);

        // Build projection for each result relation
        resultRelInfo = mtstate->resultRelInfo;
        foreach(l, node->returningLists) {
            List *rlist = (List *) lfirst(l);
            resultRelInfo->ri_returningList = rlist;
            resultRelInfo->ri_projectReturning =
                ExecBuildProjectionInfo(rlist, mtstate->ps.ps_ExprContext,
                                      mtstate->ps.ps_ResultTupleSlot, &mtstate->ps,
                                      resultRelInfo->ri_RelationDesc->rd_att);
            resultRelInfo++;
        }
    } else {
        // Create dummy result tuple type
        mtstate->ps.plan->targetlist = NIL;
        ExecInitResultTypeTL(&mtstate->ps);
        mtstate->ps.ps_ExprContext = NULL;
    }

    // Set up ON CONFLICT handling for INSERT operations
    if (node->onConflictAction != ONCONFLICT_NONE) {
        resultRelInfo = mtstate->resultRelInfo;
        resultRelInfo->ri_onConflictArbiterIndexes = node->arbiterIndexes;

        if (node->onConflictAction == ONCONFLICT_UPDATE) {
            OnConflictSetState *onconfl = makeNode(OnConflictSetState);

            if (mtstate->ps.ps_ExprContext == NULL)
                ExecAssignExprContext(estate, &mtstate->ps);

            resultRelInfo->ri_onConflict = onconfl;

            // Create slots for existing and projected tuples
            onconfl->oc_Existing = table_slot_create(resultRelInfo->ri_RelationDesc,
                                                   &mtstate->ps.state->es_tupleTable);
            onconfl->oc_ProjSlot = table_slot_create(resultRelInfo->ri_RelationDesc,
                                                   &mtstate->ps.state->es_tupleTable);

            // Build UPDATE SET projection
            onconfl->oc_ProjInfo =
                ExecBuildUpdateProjection(node->onConflictSet, true,
                                        node->onConflictCols,
                                        resultRelInfo->ri_RelationDesc->rd_att,
                                        mtstate->ps.ps_ExprContext,
                                        onconfl->oc_ProjSlot, &mtstate->ps);

            // Initialize WHERE clause if present
            if (node->onConflictWhere) {
                onconfl->oc_WhereClause = ExecInitQual((List *) node->onConflictWhere,
                                                     &mtstate->ps);
            }
        }
    }

    // Set up auxiliary row marks for EPQ in UPDATE/DELETE/MERGE
    List *arowmarks = NIL;
    foreach(l, node->rowMarks) {
        PlanRowMark *rc = lfirst_node(PlanRowMark, l);
        if (!rc->isParent) {  // Skip parent rowmarks
            ExecRowMark *erm = ExecFindRowMark(estate, rc->rti, false);
            ExecAuxRowMark *aerm = ExecBuildAuxRowMark(erm, subplan->targetlist);
            arowmarks = lappend(arowmarks, aerm);
        }
    }

    // Initialize MERGE command state
    if (mtstate->operation == CMD_MERGE)
        ExecInitMerge(mtstate, estate);

    // Complete EPQ setup with subplan and row marks
    EvalPlanQualSetPlan(&mtstate->mt_epqstate, subplan, arowmarks);

    return mtstate;
}
```