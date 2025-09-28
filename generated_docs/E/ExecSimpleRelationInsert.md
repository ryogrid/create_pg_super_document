# ExecSimpleRelationInsert

## Location
[src/backend/executor/execReplication.c:490-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L490-L553)

## Overview
ExecSimpleRelationInsert performs a complete tuple insertion into a relation, including constraint checking, trigger execution, index maintenance, and stored generated column computation.

## Definition
```c
void ExecSimpleRelationInsert(ResultRelInfo *resultRelInfo,
                             EState *estate, TupleTableSlot *slot)
```

## Detailed Description
This function provides a comprehensive tuple insertion workflow that handles all aspects of inserting a tuple into a PostgreSQL relation. It is specifically designed for replication scenarios where a complete, safe insertion process is required.

The function follows a structured workflow: first validating replica identity requirements, then executing BEFORE ROW INSERT triggers which may skip the insertion. If the insertion proceeds, it computes stored generated columns, validates all constraints (including partition constraints), performs the actual tuple insertion, maintains indexes, and finally executes AFTER ROW INSERT triggers.

The function assumes the caller has already opened any required indexes and is responsible for their management. It currently supports only regular tables (RELKIND_RELATION) and includes comprehensive error handling and validation.

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo structure containing relation metadata, trigger descriptions, and index information
- `estate`: Executor state containing transaction context and execution environment
- `slot`: TupleTableSlot containing the tuple data to be inserted

## Dependencies
- Functions called/Symbols referenced:
  - [CheckCmdReplicaIdentity](../C/CheckCmdReplicaIdentity.md): Validates replica identity requirements for INSERT operations
  - [ExecBRInsertTriggers](ExecBRInsertTriggers.md): Executes BEFORE ROW INSERT triggers
  - [ExecComputeStoredGenerated](ExecComputeStoredGenerated.md): Computes values for stored generated columns
  - [ExecConstraints](ExecConstraints.md): Validates tuple constraints
  - [ExecPartitionCheck](ExecPartitionCheck.md): Validates partition constraints if applicable
  - [simple_table_tuple_insert](../s/simple_table_tuple_insert.md): Performs the actual tuple insertion into the table
  - [ExecInsertIndexTuples](ExecInsertIndexTuples.md): Creates index entries for the inserted tuple
  - [ExecARInsertTriggers](ExecARInsertTriggers.md): Executes AFTER ROW INSERT triggers
- Called from (representative examples):
  - [apply_handle_insert_internal](../a/apply_handle_insert_internal.md): Logical replication worker for processing INSERT operations
  - [exec_rt_fetch](../e/exec_rt_fetch.md): Through executor header inclusion

## Notes and Other Information
- Currently limited to regular table relations (RELKIND_RELATION)
- Caller must ensure indexes are properly opened before calling this function
- Includes a skip mechanism through BEFORE triggers that can prevent the actual insertion
- Handles both regular and partition table constraints appropriately
- Does not currently capture transition tuples for statement-level triggers (noted as XXX comment)
- Used primarily in logical replication contexts where complete insertion semantics are required
- Provides comprehensive error handling through constraint validation and trigger execution

## Simplified Source

```c
// Simplified version of ExecSimpleRelationInsert
void ExecSimpleRelationInsert(ResultRelInfo *resultRelInfo,
                              EState *estate, TupleTableSlot *slot) {
    bool skip_tuple = false;
    Relation rel = resultRelInfo->ri_RelationDesc;

    // Validate this is a regular table
    Assert(rel->rd_rel->relkind == RELKIND_RELATION);

    // Check replica identity for INSERT operation
    CheckCmdReplicaIdentity(rel, CMD_INSERT);

    // Execute BEFORE INSERT triggers
    if (resultRelInfo->ri_TrigDesc &&
        resultRelInfo->ri_TrigDesc->trig_insert_before_row) {
        if (!ExecBRInsertTriggers(estate, resultRelInfo, slot)) {
            skip_tuple = true;  // Trigger says skip this tuple
        }
    }

    // Proceed with insertion if not skipped
    if (!skip_tuple) {
        List *recheckIndexes = NIL;

        // Compute stored generated columns
        if (rel->rd_att->constr && rel->rd_att->constr->has_generated_stored) {
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);
        }

        // Validate constraints
        if (rel->rd_att->constr) {
            ExecConstraints(resultRelInfo, slot, estate);
        }
        if (rel->rd_rel->relispartition) {
            ExecPartitionCheck(resultRelInfo, slot, estate, true);
        }

        // Insert the tuple
        simple_table_tuple_insert(resultRelInfo->ri_RelationDesc, slot);

        // Update indexes
        if (resultRelInfo->ri_NumIndices > 0) {
            recheckIndexes = ExecInsertIndexTuples(resultRelInfo, slot, estate,
                                                   false, false, NULL, NIL, false);
        }

        // Execute AFTER INSERT triggers
        ExecARInsertTriggers(estate, resultRelInfo, slot, recheckIndexes, NULL);

        list_free(recheckIndexes);
    }
}
```

Key simplifications made:
- Added clear comments for each major step in the insertion process
- Emphasized the conditional flow based on trigger results
- Preserved all essential validation and computation steps
- Highlighted the relationship between constraints, indexes, and triggers