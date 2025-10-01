# CheckRelationTableSpaceMove

## Location
[src/backend/commands/tablecmds.c:3561-3617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3561-L3617)

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

## Simplified Source

```c
bool CheckRelationTableSpaceMove(Relation rel, Oid newTableSpaceId) {
    Oid oldTableSpaceId;

    // Check if this would be a no-op (MyDatabaseTableSpace is stored as 0)
    oldTableSpaceId = rel->rd_rel->reltablespace;
    if (newTableSpaceId == oldTableSpaceId ||
        (newTableSpaceId == MyDatabaseTableSpace && oldTableSpaceId == 0))
        return false;

    // Cannot move mapped relations (system catalogs)
    if (RelationIsMapped(rel))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot move system relation \"%s\"",
                              RelationGetRelationName(rel))));

    // Cannot move non-shared relations to pg_global
    if (newTableSpaceId == GLOBALTABLESPACE_OID)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("only shared relations can be placed in pg_global tablespace")));

    // Cannot move temp tables from other sessions
    if (RELATION_IS_OTHER_TEMP(rel))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot move temporary tables of other sessions")));

    return true;
}
```