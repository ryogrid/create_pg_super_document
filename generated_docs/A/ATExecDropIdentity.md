# ATExecDropIdentity

## Location
[src/backend/commands/tablecmds.c:8246-8359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8246-L8359)

## Overview
ATExecDropIdentity implements the ALTER TABLE ALTER COLUMN DROP IDENTITY command, removing identity column properties from a column and cleaning up associated sequences in PostgreSQL relations.

## Definition
```c
static ObjectAddress ATExecDropIdentity(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode, bool recurse, bool recursing)
```

## Detailed Description
This function removes identity column properties from an existing column, effectively converting it back to a regular column. The operation involves updating the column's attributes in the system catalogs and cleaning up the associated sequence object that was automatically created for the identity column.

The function performs validation and handles several scenarios:
- Validates that the column exists and is not a system column
- Checks if the column is actually an identity column (with optional graceful handling via missing_ok)
- For partitioned tables, ensures the operation applies to all partitions when recurse is set
- Prevents direct operations on individual partitions without going through the parent

The cleanup process includes removing the identity flag from pg_attribute and deleting the internal sequence that was automatically created for the identity column. The sequence deletion includes proper dependency cleanup to maintain referential integrity.

## Parameters / Member Variables
- `rel`: The relation containing the identity column to drop
- `colName`: Name of the identity column to remove
- `missing_ok`: If true, issues a NOTICE rather than ERROR when column is not an identity column
- `lockmode`: Lock mode to use when accessing child relations
- `recurse`: Whether to apply changes to partitioned table children
- `recursing`: Internal flag indicating this is a recursive call to a partition

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSubSet
  - [heap_freetuple](../h/heap_freetuple.md)
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [getIdentitySequence](../g/getIdentitySequence.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [performDeletion](../p/performDeletion.md)
  - [ATExecDropIdentity](ATExecDropIdentity.md) (recursive call)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [ATExecDropIdentity](ATExecDropIdentity.md) (self-recursion)

## Notes and Other Information
- This is a static function within tablecmds.c, part of the ALTER TABLE infrastructure
- Supports graceful handling of non-identity columns when missing_ok is true (issues NOTICE instead of ERROR)
- The identity property is not inherited through regular table inheritance, only through partitioning
- Automatically locates and deletes the internal sequence associated with the identity column
- Uses proper dependency management to ensure sequence cleanup doesn't break referential integrity
- For partitioned tables, recursively removes identity from all child partitions
- Sequence deletion only occurs in the top-level call (not during recursion) to avoid duplicate cleanup
- Sets the attidentity field to '\0' (null character) to indicate the column is no longer an identity column
- The function is also used during partition detachment operations to clean up identity sequences

## Simplified Source

```c
static ObjectAddress
ATExecDropIdentity(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode,
                   bool recurse, bool recursing)
{
    HeapTuple tuple;
    Form_pg_attribute attTup;
    AttrNumber attnum;
    Relation attrelation;
    ObjectAddress address;
    Oid seqid;
    ObjectAddress seqaddress;
    bool ispartitioned;

    // Validate partitioned table operations
    ispartitioned = (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
    if (ispartitioned && !recurse)
        ereport(ERROR, (errmsg("cannot drop identity from a column of only the partitioned table")));

    if (rel->rd_rel->relispartition && !recursing)
        ereport(ERROR, (errmsg("cannot drop identity from a column of a partition")));

    // Open attribute catalog and find the column
    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmsg("column \"%s\" of relation \"%s\" does not exist",
                               colName, RelationGetRelationName(rel))));

    attTup = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = attTup->attnum;

    // Validate column type
    if (attnum <= 0)
        ereport(ERROR, (errmsg("cannot alter system column \"%s\"", colName)));

    // Check if column is an identity column
    if (!attTup->attidentity)
    {
        if (missing_ok)
        {
            ereport(NOTICE, (errmsg("column \"%s\" is not an identity column, skipping", colName)));
            heap_freetuple(tuple);
            table_close(attrelation, RowExclusiveLock);
            return InvalidObjectAddress;
        }
        ereport(ERROR, (errmsg("column \"%s\" is not an identity column", colName)));
    }

    // Remove identity flag from column
    attTup->attidentity = '\0';
    CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

    // Notify other subsystems and prepare return value
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attTup->attnum);
    ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);

    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    // Handle partitioned tables - recurse to children
    if (recurse && ispartitioned)
    {
        List *children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        ListCell *lc;

        foreach(lc, children)
        {
            Relation childrel = table_open(lfirst_oid(lc), NoLock);
            ATExecDropIdentity(childrel, colName, false, lockmode, recurse, true);
            table_close(childrel, NoLock);
        }
    }

    // Clean up the identity sequence (only at top level)
    if (!recursing)
    {
        // Find and delete the associated identity sequence
        seqid = getIdentitySequence(rel, attnum, false);
        deleteDependencyRecordsForClass(RelationRelationId, seqid,
                                        RelationRelationId, DEPENDENCY_INTERNAL);
        CommandCounterIncrement();

        seqaddress.classId = RelationRelationId;
        seqaddress.objectId = seqid;
        seqaddress.objectSubId = 0;
        performDeletion(&seqaddress, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }

    return address;
}
```