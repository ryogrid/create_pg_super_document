# ATExecSetTableSpace

## Location
[src/backend/commands/tablecmds.c:15253-15345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15253-L15345)

## Overview
ATExecSetTableSpace executes the ALTER TABLE SET TABLESPACE command by physically moving table data to a new tablespace without tuple rewriting, optimizing for fast data copy operations.

## Definition
```c
static void ATExecSetTableSpace(Oid tableOid, Oid newTableSpace, LOCKMODE lockmode)
```

## Detailed Description
This function implements the execution phase for moving a table to a different tablespace when no tuple rewriting is required. It performs the actual data movement by creating new storage in the target tablespace, copying data using access method-specific functions, updating system catalogs, and recursively handling associated TOAST tables and indexes.

The function operates by opening the relation, validating the move operation, allocating a new relfilenumber in the target tablespace, copying data using either index_copy_data() or table_relation_copy_data() depending on the relation kind, updating the pg_class catalog, and recursively processing any associated TOAST relations and indexes. This approach ensures atomicity and consistency while optimizing for performance when tuple rewriting is not needed.

## Parameters / Member Variables
- `tableOid`: Object identifier of the table/relation to move
- `newTableSpace`: Object identifier of the destination tablespace  
- `lockmode`: Lock mode to acquire on the relations being moved

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md): Opens relations for access
  - [relation_close](../r/relation_close.md): Closes relations and releases locks
  - [CheckRelationTableSpaceMove](../C/CheckRelationTableSpaceMove.md): Validates whether the move operation is allowed
  - [GetNewRelFileNumber](../G/GetNewRelFileNumber.md): Allocates a new relfilenumber in the target tablespace
  - [index_copy_data](../i/index_copy_data.md): Copies index data to new storage location
  - [table_relation_copy_data](../t/table_relation_copy_data.md): Copies table data to new storage location
  - [SetRelationTableSpace](../S/SetRelationTableSpace.md): Updates pg_class with new tablespace and relfilenumber
  - [RelationGetIndexList](../R/RelationGetIndexList.md): Gets list of indexes for TOAST relations
  - [RelationAssumeNewRelfilelocator](../R/RelationAssumeNewRelfilelocator.md): Updates relation's internal locator information
  - InvokeObjectPostAlterHook: Triggers post-alter hooks for dependency tracking
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Makes catalog changes visible to subsequent operations

- Called from (representative examples):
  - [ATRewriteTables](ATRewriteTables.md): Main table rewriting function during ALTER TABLE
  - [ATExecSetTableSpace](ATExecSetTableSpace.md): Recursive calls for TOAST tables and indexes

## Notes and Other Information
- Recursively processes TOAST tables and their indexes to ensure complete tablespace migration
- Allocates new relfilenumbers to avoid conflicts across tablespaces
- Uses access method-specific copy functions for optimal performance (index_copy_data vs table_relation_copy_data)
- Handles both regular tables and indexes through different code paths
- Updates relation catalog information atomically to maintain consistency
- Includes safety check via CheckRelationTableSpaceMove to prevent invalid operations
- Does not work on pg_class itself or its indexes due to bootstrap constraints
- Ensures visibility of changes through CommandCounterIncrement before processing related objects
- Maintains proper locking throughout the operation to prevent concurrent access issues

## Simplified Source

```c
static void
ATExecSetTableSpace(Oid tableOid, Oid newTableSpace, LOCKMODE lockmode)
{
    Relation rel;
    Oid reltoastrelid;
    RelFileNumber newrelfilenumber;
    RelFileLocator newrlocator;
    List *reltoastidxids = NIL;
    ListCell *lc;

    // Open relation and validate the move operation
    rel = relation_open(tableOid, lockmode);

    if (!CheckRelationTableSpaceMove(rel, newTableSpace))
    {
        InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
        relation_close(rel, NoLock);
        return;
    }

    // Get TOAST table info if present
    reltoastrelid = rel->rd_rel->reltoastrelid;
    if (OidIsValid(reltoastrelid))
    {
        Relation toastRel = relation_open(reltoastrelid, lockmode);
        reltoastidxids = RelationGetIndexList(toastRel);
        relation_close(toastRel, lockmode);
    }

    // Allocate new relfilenumber in target tablespace
    newrelfilenumber = GetNewRelFileNumber(newTableSpace, NULL,
                                           rel->rd_rel->relpersistence);

    // Set up new relation locator
    newrlocator = rel->rd_locator;
    newrlocator.relNumber = newrelfilenumber;
    newrlocator.spcOid = newTableSpace;

    // Copy data using appropriate access method
    if (rel->rd_rel->relkind == RELKIND_INDEX)
        index_copy_data(rel, newrlocator);
    else
        table_relation_copy_data(rel, &newrlocator);

    // Update system catalog
    SetRelationTableSpace(rel, newTableSpace, newrelfilenumber);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
    RelationAssumeNewRelfilelocator(rel);
    relation_close(rel, NoLock);

    // Make changes visible
    CommandCounterIncrement();

    // Recursively move TOAST table and indexes
    if (OidIsValid(reltoastrelid))
        ATExecSetTableSpace(reltoastrelid, newTableSpace, lockmode);

    foreach(lc, reltoastidxids)
        ATExecSetTableSpace(lfirst_oid(lc), newTableSpace, lockmode);

    list_free(reltoastidxids);
}
```