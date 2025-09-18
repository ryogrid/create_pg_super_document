# CheckValidResultRel

## Location
[src/backend/executor/execMain.c:1019-1130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1019-L1130)

## Overview
Validates that a proposed result relation is a legal target for the specified database operation, ensuring compatibility between the relation type and the intended command.

## Definition


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