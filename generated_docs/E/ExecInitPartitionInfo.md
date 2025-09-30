# ExecInitPartitionInfo

## Location
[src/backend/executor/execPartition.c:495-985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L495-L985)

## Overview
Locks a partition and initializes a complete ResultRelInfo structure for it, setting up all necessary execution state including indexes, constraints, projections, and conflict handling for INSERT/UPDATE/MERGE operations.

## Definition

```c
static ResultRelInfo *
ExecInitPartitionInfo(ModifyTableState *mtstate, EState *estate,
					  PartitionTupleRouting *proute,
					  PartitionDispatch dispatch,
					  ResultRelInfo *rootResultRelInfo,
					  int partidx)
```
## Detailed Description
This function performs comprehensive initialization of a partition's execution state when it's first accessed during tuple routing. It opens the partition relation with appropriate locks, creates and configures a ResultRelInfo structure, and sets up all execution components including indexes, WITH CHECK OPTION constraints, RETURNING projections, ON CONFLICT handling, and MERGE operation state. The function handles attribute number mapping between the root table and partition when they have different tuple descriptors, ensuring that expressions and projections work correctly across the partition hierarchy.

The initialization process is comprehensive, covering all aspects of DML operations that might be performed on the partition. This includes setting up speculative insertion capabilities for ON CONFLICT handling, translating column references in constraints and projections, and preparing merge action states for MERGE operations.

## Parameters / Member Variables
- : ModifyTableState containing the modify operation context and reusable ResultRelInfo structures
- : Executor state providing memory contexts, expression evaluation environment, and tuple table management
- : PartitionTupleRouting structure where the new ResultRelInfo will be stored for future reuse
- : PartitionDispatch information for the current partitioning level
- : ResultRelInfo for the root table, used as a template for constraint and projection setup
- : Index of the target partition within the current dispatch level

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (ResultRelInfo creation)
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
  - [CheckValidResultRel](../C/CheckValidResultRel.md)
  - [ExecOpenIndices](ExecOpenIndices.md)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
  - [map_variable_attnos](../m/map_variable_attnos.md)
  - [ExecInitQual](ExecInitQual.md)
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md)
  - [ExecInitRoutingInfo](ExecInitRoutingInfo.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [get_partition_ancestors](../g/get_partition_ancestors.md)
  - [ExecGetRootToChildMap](ExecGetRootToChildMap.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
  - [ExecInitMergeTupleSlots](ExecInitMergeTupleSlots.md)
  - [adjust_partition_colnos](../a/adjust_partition_colnos.md)
  - [adjust_partition_colnos_using_map](../a/adjust_partition_colnos_using_map.md)
- Called from (representative examples):
  - [ExecFindPartition](ExecFindPartition.md) (in execPartition.c:373)

## Notes and Other Information
This is a static function that handles the complete initialization of partition execution state. It's designed to be called lazily when a partition is first accessed, supporting efficient partition pruning. The function handles complex attribute mapping scenarios when partitions have different tuple descriptors from their parent tables. It also manages proper memory context switching to ensure allocations are made in the appropriate context for the partition routing infrastructure. The function supports all DML operations (INSERT, UPDATE, DELETE, MERGE) and their associated features like conflict resolution and constraint checking.

## Simplified Source

```c
static ResultRelInfo *
ExecInitPartitionInfo(ModifyTableState *mtstate, EState *estate,
                      PartitionTupleRouting *proute,
                      PartitionDispatch dispatch,
                      ResultRelInfo *rootResultRelInfo,
                      int partidx)
{
    ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    Oid         partOid = dispatch->partdesc->oids[partidx];
    Relation    partrel;
    Relation    firstResultRel = mtstate->resultRelInfo[0].ri_RelationDesc;
    ResultRelInfo *leaf_part_rri;
    MemoryContext oldcxt;
    AttrMap    *part_attmap = NULL;
    bool        found_whole_row;

    oldcxt = MemoryContextSwitchTo(proute->memcxt);

    // Open partition and create ResultRelInfo
    partrel = table_open(partOid, RowExclusiveLock);
    leaf_part_rri = makeNode(ResultRelInfo);
    InitResultRelInfo(leaf_part_rri, partrel, 0, rootResultRelInfo, estate->es_instrument);

    // Validate partition as valid target for INSERT
    CheckValidResultRel(leaf_part_rri, CMD_INSERT, NIL);

    // Open partition indices for conflict detection
    if (partrel->rd_rel->relhasindex && leaf_part_rri->ri_IndexRelationDescs == NULL)
        ExecOpenIndices(leaf_part_rri, (node != NULL && node->onConflictAction != ONCONFLICT_NONE));

    // Set up WITH CHECK OPTION constraints
    if (node && node->withCheckOptionLists != NIL) {
        List *wcoList = linitial(node->withCheckOptionLists);
        List *wcoExprs = NIL;
        ListCell *ll;

        // Map attribute numbers from root to partition
        part_attmap = build_attrmap_by_name(RelationGetDescr(partrel),
                                           RelationGetDescr(firstResultRel), false);
        wcoList = (List *) map_variable_attnos((Node *) wcoList,
                                              mtstate->resultRelInfo[0].ri_RangeTableIndex, 0,
                                              part_attmap,
                                              RelationGetForm(partrel)->reltype,
                                              &found_whole_row);

        // Initialize constraint expressions
        foreach(ll, wcoList) {
            WithCheckOption *wco = lfirst_node(WithCheckOption, ll);
            ExprState *wcoExpr = ExecInitQual(castNode(List, wco->qual), &mtstate->ps);
            wcoExprs = lappend(wcoExprs, wcoExpr);
        }

        leaf_part_rri->ri_WithCheckOptions = wcoList;
        leaf_part_rri->ri_WithCheckOptionExprs = wcoExprs;
    }

    // Set up RETURNING projection
    if (node && node->returningLists != NIL) {
        List *returningList = linitial(node->returningLists);

        // Map attribute numbers for RETURNING list
        if (part_attmap == NULL)
            part_attmap = build_attrmap_by_name(RelationGetDescr(partrel),
                                               RelationGetDescr(firstResultRel), false);

        returningList = (List *) map_variable_attnos((Node *) returningList,
                                                    mtstate->resultRelInfo[0].ri_RangeTableIndex, 0,
                                                    part_attmap,
                                                    RelationGetForm(partrel)->reltype,
                                                    &found_whole_row);

        leaf_part_rri->ri_returningList = returningList;
        leaf_part_rri->ri_projectReturning =
            ExecBuildProjectionInfo(returningList, mtstate->ps.ps_ExprContext,
                                   mtstate->ps.ps_ResultTupleSlot,
                                   &mtstate->ps, RelationGetDescr(partrel));
    }

    // Set up tuple routing information
    ExecInitRoutingInfo(mtstate, estate, proute, dispatch, leaf_part_rri, partidx, false);

    // Set up ON CONFLICT handling
    if (node && node->onConflictAction != ONCONFLICT_NONE) {
        // Map arbiter indexes from root to partition
        if (rootResultRelInfo->ri_onConflictArbiterIndexes != NIL) {
            List *childIdxs = RelationGetIndexList(leaf_part_rri->ri_RelationDesc);
            List *arbiterIndexes = NIL;
            ListCell *lc, *lc2;

            foreach(lc, childIdxs) {
                Oid childIdx = lfirst_oid(lc);
                List *ancestors = get_partition_ancestors(childIdx);
                foreach(lc2, rootResultRelInfo->ri_onConflictArbiterIndexes) {
                    if (list_member_oid(ancestors, lfirst_oid(lc2)))
                        arbiterIndexes = lappend_oid(arbiterIndexes, childIdx);
                }
                list_free(ancestors);
            }
            leaf_part_rri->ri_onConflictArbiterIndexes = arbiterIndexes;
        }

        // Set up DO UPDATE state if needed
        if (node->onConflictAction == ONCONFLICT_UPDATE) {
            OnConflictSetState *onconfl = makeNode(OnConflictSetState);
            TupleConversionMap *map = ExecGetRootToChildMap(leaf_part_rri, estate);

            leaf_part_rri->ri_onConflict = onconfl;
            onconfl->oc_Existing = table_slot_create(leaf_part_rri->ri_RelationDesc,
                                                    &mtstate->ps.state->es_tupleTable);

            // Reuse parent state if tuple descriptors match, otherwise create new
            if (map == NULL) {
                onconfl->oc_ProjSlot = rootResultRelInfo->ri_onConflict->oc_ProjSlot;
                onconfl->oc_ProjInfo = rootResultRelInfo->ri_onConflict->oc_ProjInfo;
                onconfl->oc_WhereClause = rootResultRelInfo->ri_onConflict->oc_WhereClause;
            } else {
                // Create partition-specific projections and where clauses
                // (detailed mapping logic simplified for readability)
                List *onconflset = copyObject(node->onConflictSet);
                // ... additional attribute mapping and projection setup ...
            }
        }
    }

    // Add to estate's tuple routing result relations list
    MemoryContextSwitchTo(estate->es_query_cxt);
    estate->es_tuple_routing_result_relations =
        lappend(estate->es_tuple_routing_result_relations, leaf_part_rri);

    // Set up MERGE operation state
    if (node && node->operation == CMD_MERGE) {
        List *firstMergeActionList = linitial(node->mergeActionLists);
        ListCell *lc;

        if (part_attmap == NULL)
            part_attmap = build_attrmap_by_name(RelationGetDescr(partrel),
                                               RelationGetDescr(firstResultRel), false);

        if (unlikely(!leaf_part_rri->ri_projectNewInfoValid))
            ExecInitMergeTupleSlots(mtstate, leaf_part_rri);

        // Initialize join condition
        Node *joinCondition = map_variable_attnos(linitial(node->mergeJoinConditions),
                                                 mtstate->resultRelInfo[0].ri_RangeTableIndex, 0,
                                                 part_attmap,
                                                 RelationGetForm(partrel)->reltype,
                                                 &found_whole_row);
        leaf_part_rri->ri_MergeJoinCondition = ExecInitQual((List *) joinCondition, &mtstate->ps);

        // Initialize merge actions (INSERT, UPDATE, DELETE, NOTHING)
        foreach(lc, firstMergeActionList) {
            MergeAction *action = copyObject(lfirst(lc));
            MergeActionState *action_state = makeNode(MergeActionState);
            action_state->mas_action = action;

            leaf_part_rri->ri_MergeActions[action->matchKind] =
                lappend(leaf_part_rri->ri_MergeActions[action->matchKind], action_state);

            // Set up action-specific projections
            switch (action->commandType) {
                case CMD_INSERT:
                    action_state->mas_proj =
                        ExecBuildProjectionInfo(action->targetList, mtstate->ps.ps_ExprContext,
                                               leaf_part_rri->ri_newTupleSlot, &mtstate->ps,
                                               RelationGetDescr(partrel));
                    break;
                case CMD_UPDATE:
                    if (part_attmap)
                        action->updateColnos = adjust_partition_colnos_using_map(action->updateColnos, part_attmap);
                    action_state->mas_proj =
                        ExecBuildUpdateProjection(action->targetList, true, action->updateColnos,
                                                 RelationGetDescr(leaf_part_rri->ri_RelationDesc),
                                                 mtstate->ps.ps_ExprContext,
                                                 leaf_part_rri->ri_newTupleSlot, NULL);
                    break;
                case CMD_DELETE:
                case CMD_NOTHING:
                    break;
            }

            // Initialize when clauses
            action->qual = map_variable_attnos(action->qual,
                                              mtstate->resultRelInfo[0].ri_RangeTableIndex, 0,
                                              part_attmap,
                                              RelationGetForm(partrel)->reltype,
                                              &found_whole_row);
            action_state->mas_whenqual = ExecInitQual((List *) action->qual, &mtstate->ps);
        }
    }

    MemoryContextSwitchTo(oldcxt);
    return leaf_part_rri;
}
```