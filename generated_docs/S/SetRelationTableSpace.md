# SetRelationTableSpace

## Location
[src/backend/commands/tablecmds.c:3618-3662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3618-L3662)

## Overview
SetRelationTableSpace updates the tablespace and optionally the file number for a relation in the pg_class system catalog, handling the low-level catalog modification aspect of tablespace moves.

## Definition
```c
void SetRelationTableSpace(Relation rel, Oid newTableSpaceId, RelFileNumber newRelFilenumber)
```

## Detailed Description
This function performs the actual catalog update when moving a relation to a new tablespace. It modifies the pg_class system catalog to update the reltablespace field and optionally the relfilenode field. The function also handles dependency tracking for relations without physical storage by calling changeDependencyOnTablespace. It assumes validation has already been performed by CheckRelationTableSpaceMove and requires the caller to make changes visible with CommandCounterIncrement.

## Parameters / Member Variables
- `rel`: The relation being moved, must be held with AccessExclusiveLock
- `newTableSpaceId`: The OID of the destination tablespace
- `newRelFilenumber`: The new file number for the relation, or InvalidRelFileNumber if not being updated

## Dependencies
- Functions called/Symbols referenced:
  - [CheckRelationTableSpaceMove](../C/CheckRelationTableSpaceMove.md)
  - [SearchSysCacheLockedCopy1](SearchSysCacheLockedCopy1.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - [changeDependencyOnTablespace](../c/changeDependencyOnTablespace.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - RELKIND_HAS_STORAGE
  - RelFileNumberIsValid
- Called from (representative examples):
  - [reindex_index](../r/reindex_index.md)
  - [ATExecSetTableSpace](../A/ATExecSetTableSpace.md)
  - [ATExecSetTableSpaceNoStorage](../A/ATExecSetTableSpaceNoStorage.md)

## Notes and Other Information
- The function asserts that CheckRelationTableSpaceMove returns true, ensuring validation was performed
- MyDatabaseTableSpace is stored as InvalidOid (0) in pg_class.reltablespace
- Dependency tracking is only updated for relations without physical storage (views, etc.)
- The caller is responsible for making the change visible and managing the overall transaction
- Uses row-exclusive lock on pg_class during the update operation

## Simplified Source

```c
void SetRelationTableSpace(Relation rel, Oid newTableSpaceId, RelFileNumber newRelFilenumber) {
    Relation pg_class;
    HeapTuple tuple;
    ItemPointerData otid;
    Form_pg_class rd_rel;
    Oid reloid = RelationGetRelid(rel);

    // Validate that move is allowed
    Assert(CheckRelationTableSpaceMove(rel, newTableSpaceId));

    // Open pg_class for modification
    pg_class = table_open(RelationRelationId, RowExclusiveLock);

    // Get the relation's pg_class tuple
    tuple = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(reloid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", reloid);
    otid = tuple->t_self;
    rd_rel = (Form_pg_class) GETSTRUCT(tuple);

    // Update tablespace (store MyDatabaseTableSpace as InvalidOid)
    rd_rel->reltablespace = (newTableSpaceId == MyDatabaseTableSpace) ?
        InvalidOid : newTableSpaceId;

    // Update file number if specified
    if (RelFileNumberIsValid(newRelFilenumber))
        rd_rel->relfilenode = newRelFilenumber;

    // Write the updated tuple
    CatalogTupleUpdate(pg_class, &otid, tuple);
    UnlockTuple(pg_class, &otid, InplaceUpdateTupleLock);

    // Update dependencies for relations without storage
    if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
        changeDependencyOnTablespace(RelationRelationId, reloid, rd_rel->reltablespace);

    // Cleanup
    heap_freetuple(tuple);
    table_close(pg_class, RowExclusiveLock);
}
```