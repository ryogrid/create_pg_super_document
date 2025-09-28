# ExecSimpleRelationUpdate

## Location
[src/backend/executor/execReplication.c:554-622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L554-L622)

## Overview
ExecSimpleRelationUpdate performs a complete tuple update operation, including constraint checking, trigger execution, index maintenance, and stored generated column computation.

## Definition
```c
void ExecSimpleRelationUpdate(ResultRelInfo *resultRelInfo,
                             EState *estate, EPQState *epqstate,
                             TupleTableSlot *searchslot, TupleTableSlot *slot)
```

## Detailed Description
This function provides a comprehensive tuple update workflow designed specifically for replication scenarios. It locates an existing tuple using the searchslot information and updates it with new data from the slot parameter, handling all associated database operations including trigger execution, constraint validation, and index maintenance.

The function follows a structured workflow: first validating that the relation is appropriate for updates (non-system, regular table), checking replica identity requirements, then executing BEFORE ROW UPDATE triggers which may skip the update. If the update proceeds, it computes stored generated columns, validates constraints including partition constraints, performs the actual tuple update, maintains indexes as needed, and finally executes AFTER ROW UPDATE triggers.

The function supports intelligent index updates through the TU_UpdateIndexes mechanism, which can optimize index maintenance based on whether indexes actually need updating (TU_None, TU_All, or TU_Summarizing).

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo structure containing relation metadata, trigger descriptions, and index information
- `estate`: Executor state containing transaction context and execution environment
- `epqstate`: EPQ (EvalPlanQual) state for handling concurrent updates and providing consistency
- `searchslot`: TupleTableSlot containing tuple identification information (including TID) for locating the tuple to update
- `slot`: TupleTableSlot containing the new tuple data to replace the existing tuple

## Dependencies
- Functions called/Symbols referenced:
  - [IsCatalogRelation](../I/IsCatalogRelation.md): Validates that the relation is not a system catalog
  - [CheckCmdReplicaIdentity](../C/CheckCmdReplicaIdentity.md): Validates replica identity requirements for UPDATE operations
  - [ExecBRUpdateTriggers](ExecBRUpdateTriggers.md): Executes BEFORE ROW UPDATE triggers
  - [ExecComputeStoredGenerated](ExecComputeStoredGenerated.md): Computes values for stored generated columns
  - [ExecConstraints](ExecConstraints.md): Validates tuple constraints
  - [ExecPartitionCheck](ExecPartitionCheck.md): Validates partition constraints if applicable
  - [simple_table_tuple_update](../s/simple_table_tuple_update.md): Performs the actual tuple update in the table
  - [ExecInsertIndexTuples](ExecInsertIndexTuples.md): Updates index entries as needed
  - [ExecARUpdateTriggers](ExecARUpdateTriggers.md): Executes AFTER ROW UPDATE triggers
- Called from (representative examples):
  - [apply_handle_update_internal](../a/apply_handle_update_internal.md): Logical replication worker for processing UPDATE operations
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md): Handles updates with tuple routing for partitioned tables
  - [exec_rt_fetch](../e/exec_rt_fetch.md): Through executor header inclusion

## Notes and Other Information
- Restricted to non-system, regular table relations (RELKIND_RELATION)
- Includes comprehensive validation against catalog relations to prevent system table updates
- Provides skip mechanism through BEFORE triggers that can prevent the actual update
- Supports intelligent index update optimization through TU_UpdateIndexes enumeration
- Uses EPQState for handling concurrent modifications and maintaining consistency
- Handles both regular and partition table constraints appropriately
- Used primarily in logical replication contexts where complete update semantics are required
- Assumes caller has opened necessary indexes and manages them appropriately

## Simplified Source

```c
// Simplified version of ExecSimpleRelationUpdate
void ExecSimpleRelationUpdate(ResultRelInfo *resultRelInfo,
                              EState *estate, EPQState *epqstate,
                              TupleTableSlot *searchslot, TupleTableSlot *slot) {
    bool skip_tuple = false;
    Relation rel = resultRelInfo->ri_RelationDesc;
    ItemPointer tid = &(searchslot->tts_tid);

    // Validate relation type (regular table, non-system)
    Assert(rel->rd_rel->relkind == RELKIND_RELATION);
    Assert(!IsCatalogRelation(rel));

    // Check replica identity for UPDATE operation
    CheckCmdReplicaIdentity(rel, CMD_UPDATE);

    // Execute BEFORE UPDATE triggers
    if (resultRelInfo->ri_TrigDesc &&
        resultRelInfo->ri_TrigDesc->trig_update_before_row) {
        if (!ExecBRUpdateTriggers(estate, epqstate, resultRelInfo,
                                  tid, NULL, slot, NULL, NULL)) {
            skip_tuple = true;  // Trigger says skip this update
        }
    }

    // Proceed with update if not skipped
    if (!skip_tuple) {
        List *recheckIndexes = NIL;
        TU_UpdateIndexes update_indexes;

        // Compute stored generated columns
        if (rel->rd_att->constr && rel->rd_att->constr->has_generated_stored) {
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_UPDATE);
        }

        // Validate constraints
        if (rel->rd_att->constr) {
            ExecConstraints(resultRelInfo, slot, estate);
        }
        if (rel->rd_rel->relispartition) {
            ExecPartitionCheck(resultRelInfo, slot, estate, true);
        }

        // Update the tuple
        simple_table_tuple_update(rel, tid, slot, estate->es_snapshot, &update_indexes);

        // Update indexes if needed
        if (resultRelInfo->ri_NumIndices > 0 && (update_indexes != TU_None)) {
            recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, estate,
                                                   true, false, NULL, NIL,
                                                   (update_indexes == TU_Summarizing));
        }

        // Execute AFTER UPDATE triggers
        ExecARUpdateTriggers(estate, resultRelInfo, NULL, NULL,
                             tid, NULL, slot, recheckIndexes, NULL, false);

        list_free(recheckIndexes);
    }
}
```

Key simplifications made:
- Added clear comments for each major step in the update process
- Emphasized the validation and trigger flow
- Highlighted the intelligent index update optimization
- Preserved all essential constraint checking and trigger execution