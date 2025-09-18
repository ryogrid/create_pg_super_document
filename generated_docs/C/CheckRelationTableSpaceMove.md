# CheckRelationTableSpaceMove

## Location
src/backend/commands/tablecmds.c: 3561 - 3617

## Overview
CheckRelationTableSpaceMove validates whether a relation can be moved to a new tablespace, performing various safety checks to ensure the operation is valid and permitted.

## Definition
```c
bool CheckRelationTableSpaceMove(Relation rel, Oid newTableSpaceId)
```

## Detailed Description
This function serves as a gatekeeper for tablespace move operations, ensuring that a relation can be safely moved to a target tablespace. It performs comprehensive validation including checking for no-op moves, verifying relation types that cannot be moved (mapped/system relations), preventing moves to inappropriate tablespaces (pg_global for non-shared relations), and blocking operations on temporary tables from other sessions. The caller must hold AccessExclusiveLock on the relation before calling this function.

## Parameters / Member Variables
- `rel`: The relation to be moved, must be opened with AccessExclusiveLock
- `newTableSpaceId`: The OID of the target tablespace for the move operation

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsMapped
  - RELATION_IS_OTHER_TEMP
  - RelationGetRelationName
  - ereport
- Called from (representative examples):
  - [reindex_index](../r/reindex_index.md)
  - [SetRelationTableSpace](../S/SetRelationTableSpace.md)
  - [ATExecSetTableSpace](../A/ATExecSetTableSpace.md)
  - [ATExecSetTableSpaceNoStorage](../A/ATExecSetTableSpaceNoStorage.md)

## Notes and Other Information
- Returns true if the move is valid and should proceed, false if the move would have no effect (same tablespace)
- Raises errors for invalid move attempts rather than returning false
- MyDatabaseTableSpace is stored as 0 in the system catalogs
- System relations (mapped relations) cannot be moved to different tablespaces
- Only shared relations can be placed in pg_global tablespace
- Temporary tables from other sessions cannot be moved due to buffer manager limitations