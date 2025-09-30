# CheckValidResultRel

## Location
[src/backend/executor/execMain.c:1019-1130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1019-L1130)

## Overview
Validates that a proposed result relation is a legal target for the specified database operation, ensuring compatibility between the relation type and the intended command.

## Definition

```c
void
CheckValidResultRel(ResultRelInfo *resultRelInfo, CmdType operation,
					List *mergeActions)
```
## Detailed Description
This function performs comprehensive validation to ensure that a relation can serve as a result relation for a given database operation. It examines the relation kind (table, view, foreign table, etc.) and verifies that the operation is supported for that type of relation.

The function handles different relation types with specific validation rules:
- **Regular tables and partitioned tables**: Checks replica identity requirements
- **Sequences and TOAST relations**: Always rejected for modifications
- **Views**: Requires appropriate INSTEAD OF triggers to be updatable
- **Materialized views**: Only allowed when incremental maintenance is enabled
- **Foreign tables**: Validates FDW capability and permissions for specific operations

For MERGE operations, the function ensures the result relation supports all possible actions that may be performed, regardless of whether they are actually executed.

## Parameters / Member Variables
- : Pointer to ResultRelInfo structure containing:
  - : The target relation descriptor
  - : Foreign data wrapper routines (for foreign tables)
  - : Lock requirements flag
- : The SQL command type (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE)
- : List of possible MERGE actions (used only for MERGE operations)

## Dependencies
- Functions called/Symbols referenced:
  - [IsInplaceUpdateRelation](../I/IsInplaceUpdateRelation.md)
  - [CheckCmdReplicaIdentity](CheckCmdReplicaIdentity.md)
  - [view_has_instead_trigger](../v/view_has_instead_trigger.md)
  - [error_view_not_updatable](../e/error_view_not_updatable.md)
  - [MatViewIncrementalMaintenanceIsEnabled](../M/MatViewIncrementalMaintenanceIsEnabled.md)
  - RelationGetRelationName
  - ereport
  - elog
- Called from (representative examples):
  - [CopyFrom](CopyFrom.md)
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md)
  - [ExecInitModifyTable](../E/ExecInitModifyTable.md)

## Notes and Other Information
- This function does not return a value; it either succeeds silently or throws an error
- The validation is performed at execution time as a safety check, even though similar checks should occur during parsing/planning
- Foreign table validation is particularly complex, checking both FDW function availability and relation-specific permissions
- For views, the function delegates to specialized view validation functions
- The function is complementary to CheckValidRowMarkRel for different types of relation validation
- Error messages are user-friendly and include the relation name for better diagnostics
- The function assumes the ResultRelInfo structure is fully initialized by InitResultRelInfo()
- MERGE operations require the most comprehensive validation since they may perform multiple operation types

## Simplified Source

```c
void CheckValidResultRel(ResultRelInfo *resultRelInfo, CmdType operation, List *mergeActions) {
    Relation resultRel = resultRelInfo->ri_RelationDesc;
    FdwRoutine *fdwroutine;

    // Validate that ResultRelInfo is properly initialized
    Assert(resultRelInfo->ri_needLockTagTuple == IsInplaceUpdateRelation(resultRel));

    switch (resultRel->rd_rel->relkind) {
        case RELKIND_RELATION:
        case RELKIND_PARTITIONED_TABLE:
            // Check replica identity requirements for regular tables
            CheckCmdReplicaIdentity(resultRel, operation);
            break;

        case RELKIND_SEQUENCE:
        case RELKIND_TOASTVALUE:
            // Sequences and TOAST tables cannot be modified
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot change %s \"%s\"",
                    (resultRel->rd_rel->relkind == RELKIND_SEQUENCE) ? "sequence" : "TOAST relation",
                    RelationGetRelationName(resultRel))));
            break;

        case RELKIND_VIEW:
            // Views require INSTEAD OF triggers to be updatable
            if (!view_has_instead_trigger(resultRel, operation, mergeActions))
                error_view_not_updatable(resultRel, operation, mergeActions, NULL);
            break;

        case RELKIND_MATVIEW:
            // Materialized views require incremental maintenance
            if (!MatViewIncrementalMaintenanceIsEnabled())
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot change materialized view \"%s\"",
                        RelationGetRelationName(resultRel))));
            break;

        case RELKIND_FOREIGN_TABLE:
            // Check FDW capabilities for each operation type
            fdwroutine = resultRelInfo->ri_FdwRoutine;

            if (operation == CMD_INSERT && !fdwroutine->ExecForeignInsert)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot insert into foreign table \"%s\"",
                        RelationGetRelationName(resultRel))));

            if (operation == CMD_UPDATE && !fdwroutine->ExecForeignUpdate)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot update foreign table \"%s\"",
                        RelationGetRelationName(resultRel))));

            if (operation == CMD_DELETE && !fdwroutine->ExecForeignDelete)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot delete from foreign table \"%s\"",
                        RelationGetRelationName(resultRel))));

            // Check FDW-specific permissions if available
            if (fdwroutine->IsForeignRelUpdatable) {
                int allowed_ops = fdwroutine->IsForeignRelUpdatable(resultRel);
                if ((allowed_ops & (1 << operation)) == 0)
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("foreign table \"%s\" does not allow this operation",
                            RelationGetRelationName(resultRel))));
            }
            break;

        default:
            // Unsupported relation type
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot change relation \"%s\"",
                    RelationGetRelationName(resultRel))));
            break;
    }
}
```