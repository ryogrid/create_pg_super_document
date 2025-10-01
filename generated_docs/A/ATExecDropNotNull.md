# ATExecDropNotNull

## Location
[src/backend/commands/tablecmds.c:7556-7690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7556-L7690)

## Overview
Executes the ALTER TABLE ALTER COLUMN DROP NOT NULL command, performing validation checks and updating the system catalog to remove the NOT NULL constraint from a column.

## Definition
```c
static ObjectAddress ATExecDropNotNull(Relation rel, const char *colName, LOCKMODE lockmode)
```

## Detailed Description
This function implements the execution phase of dropping a NOT NULL constraint from a table column. It performs comprehensive validation to ensure the operation is safe and maintains database integrity. The function checks that the column exists, is not a system column, is not an identity column, is not part of a primary key or replica identity index, and for partitioned tables, ensures the parent table doesn't enforce NOT NULL on the same column.

The function returns the ObjectAddress of the modified column if the constraint was actually removed, or InvalidObjectAddress if the column was already nullable. It handles the catalog update by modifying the attnotnull field in pg_attribute and triggers post-alter hooks for proper event handling.

## Parameters / Member Variables
- `rel`: The relation (table) being altered
- `colName`: The name of the column to remove NOT NULL constraint from
- `lockmode`: The lock mode to use for the operation

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (opens system catalog tables)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md) (searches for attribute by name)
  - HeapTupleIsValid (validates heap tuple)
  - RelationGetRelid (gets relation OID)
  - RelationGetRelationName (gets relation name)
  - GETSTRUCT (extracts structure from heap tuple)
  - ereport/errmsg (error reporting)
  - [RelationGetIndexList](../R/RelationGetIndexList.md) (gets list of indexes on relation)
  - foreach_oid (iterates over OID list)
  - [SearchSysCache1](../S/SearchSysCache1.md) (searches system cache)
  - elog (error logging)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache entries)
  - [list_free](../l/list_free.md) (frees list memory)
  - [get_partition_parent](../g/get_partition_parent.md) (gets parent of partition)
  - [get_attnum](../g/get_attnum.md) (gets attribute number by name)
  - TupleDescAttr (accesses tuple descriptor attributes)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (updates system catalog)
  - ObjectAddressSubSet (sets object address components)
  - InvokeObjectPostAlterHook (triggers post-alter hooks)
  - [table_close](../t/table_close.md) (closes system catalog tables)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command execution dispatcher)

## Notes and Other Information
- The function is static, meaning it's only used within tablecmds.c
- Returns ObjectAddress of modified column or InvalidObjectAddress if no change made
- Performs extensive validation including primary key, replica identity, and partition hierarchy checks
- Uses RowExclusiveLock on pg_attribute for catalog updates
- Prevents dropping NOT NULL from system columns (attnum <= 0)
- Prevents dropping NOT NULL from identity columns
- For partitions, ensures parent table doesn't enforce NOT NULL on the same column
- Triggers InvokeObjectPostAlterHook for proper event notification
- Part of PostgreSQL's ALTER TABLE infrastructure (execution phase)

## Simplified Source

```c
static ObjectAddress
ATExecDropNotNull(Relation rel, const char *colName, LOCKMODE lockmode)
{
    HeapTuple tuple;
    Form_pg_attribute attTup;
    AttrNumber attnum;
    Relation attr_rel;
    ObjectAddress address;

    // Open pg_attribute catalog for updates
    attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

    // Find the column in the system catalog
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                       errmsg("column \"%s\" of relation \"%s\" does not exist",
                              colName, RelationGetRelationName(rel))));

    attTup = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = attTup->attnum;

    // Validate column can be modified
    if (attnum <= 0)  // System columns
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("cannot alter system column \"%s\"", colName)));

    if (attTup->attidentity)  // Identity columns
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("column \"%s\" of relation \"%s\" is an identity column",
                              colName, RelationGetRelationName(rel))));

    // Check if column is part of primary key or replica identity
    List *indexoidlist = RelationGetIndexList(rel);
    foreach_oid(indexoid, indexoidlist)
    {
        HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        Form_pg_index indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

        // Check primary key and replica identity indexes
        if (indexStruct->indisprimary || indexStruct->indisreplident)
        {
            for (int i = 0; i < indexStruct->indnkeyatts; i++)
            {
                if (indexStruct->indkey.values[i] == attnum)
                {
                    const char *msg = indexStruct->indisprimary ?
                        "column \"%s\" is in a primary key" :
                        "column \"%s\" is in index used as replica identity";
                    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                   errmsg(msg, colName)));
                }
            }
        }
        ReleaseSysCache(indexTuple);
    }
    list_free(indexoidlist);

    // For partitions, check parent table constraints
    if (rel->rd_rel->relispartition)
    {
        Oid parentId = get_partition_parent(RelationGetRelid(rel), false);
        Relation parent = table_open(parentId, AccessShareLock);
        AttrNumber parent_attnum = get_attnum(parentId, colName);

        if (TupleDescAttr(RelationGetDescr(parent), parent_attnum - 1)->attnotnull)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("column \"%s\" is marked NOT NULL in parent table", colName)));
        table_close(parent, AccessShareLock);
    }

    // Update the catalog if column is currently NOT NULL
    if (attTup->attnotnull)
    {
        attTup->attnotnull = false;
        CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);
        ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    }
    else
    {
        address = InvalidObjectAddress;  // Column was already nullable
    }

    // Trigger post-alter hooks and cleanup
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    table_close(attr_rel, RowExclusiveLock);

    return address;
}
```