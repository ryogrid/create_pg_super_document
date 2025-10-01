# change_owner_fix_column_acls

## Location
[src/backend/commands/tablecmds.c:14717-14781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L14717-L14781)

## Overview
change_owner_fix_column_acls is a helper function for ATExecChangeOwner that scans all columns of a table and updates any non-null column-level access control lists (ACLs) to reflect the new table owner.

## Definition
```c
static void
change_owner_fix_column_acls(Oid relationOid, Oid oldOwnerId, Oid newOwnerId)
```

## Detailed Description
This function is responsible for updating column-level permissions when a table's ownership changes. It systematically scans through all columns of the specified relation in the pg_attribute system catalog, identifies columns that have explicit ACLs set (non-null attacl values), and updates those ACLs to reflect the ownership change using the aclnewowner function. The function skips dropped columns and only processes columns that actually have ACL entries, as null ACLs inherit permissions from the table level and don't require updates.

The function uses a system catalog scan to iterate through all attributes of the relation, extracts existing ACL information, transforms it for the new owner, and updates the catalog entry. This ensures that column-level permissions are properly maintained when table ownership changes.

## Parameters
- `relationOid`: OID of the relation whose column ACLs need to be updated
- `oldOwnerId`: OID of the previous owner (used by aclnewowner for proper ACL transformation)
- `newOwnerId`: OID of the new owner

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_close
  - [ScanKeyInit](../S/ScanKeyInit.md), systable_beginscan, systable_getnext, systable_endscan
  - [heap_getattr](../h/heap_getattr.md), heap_modify_tuple, heap_freetuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [aclnewowner](../a/aclnewowner.md), DatumGetAclP
  - Form_pg_attribute, SysScanDesc, Acl
- Called from:
  - [ATExecChangeOwner](../A/ATExecChangeOwner.md)

## Notes and Other Information
- Only processes columns with explicit ACLs (non-null attacl values)
- Skips dropped columns (attisdropped = true) as they don't need ACL updates
- Uses AttributeRelidNumIndexId for efficient scanning by relation OID
- Maintains transactional consistency by updating the catalog within the same transaction
- Essential for preserving column-level security permissions during ownership transfers
- Works in conjunction with relation-level ACL updates performed by the calling function

## Simplified Source

```c
static void
change_owner_fix_column_acls(Oid relationOid, Oid oldOwnerId, Oid newOwnerId)
{
    Relation attRelation;
    SysScanDesc scan;
    ScanKeyData key[1];
    HeapTuple attributeTuple;

    // Open pg_attribute catalog for scanning
    attRelation = table_open(AttributeRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0],
                Anum_pg_attribute_attrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relationOid));
    scan = systable_beginscan(attRelation, AttributeRelidNumIndexId,
                              true, NULL, 1, key);

    // Process each column of the relation
    while (HeapTupleIsValid(attributeTuple = systable_getnext(scan)))
    {
        Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
        Datum repl_val[Natts_pg_attribute];
        bool repl_null[Natts_pg_attribute];
        bool repl_repl[Natts_pg_attribute];
        Acl *newAcl;
        Datum aclDatum;
        bool isNull;
        HeapTuple newtuple;

        // Skip dropped columns
        if (att->attisdropped)
            continue;

        // Get existing ACL, skip if null
        aclDatum = heap_getattr(attributeTuple,
                                Anum_pg_attribute_attacl,
                                RelationGetDescr(attRelation),
                                &isNull);
        if (isNull)
            continue;

        // Update ACL with new owner
        memset(repl_null, false, sizeof(repl_null));
        memset(repl_repl, false, sizeof(repl_repl));

        newAcl = aclnewowner(DatumGetAclP(aclDatum),
                             oldOwnerId, newOwnerId);
        repl_repl[Anum_pg_attribute_attacl - 1] = true;
        repl_val[Anum_pg_attribute_attacl - 1] = PointerGetDatum(newAcl);

        // Update the catalog tuple
        newtuple = heap_modify_tuple(attributeTuple,
                                     RelationGetDescr(attRelation),
                                     repl_val, repl_null, repl_repl);

        CatalogTupleUpdate(attRelation, &newtuple->t_self, newtuple);
        heap_freetuple(newtuple);
    }

    systable_endscan(scan);
    table_close(attRelation, RowExclusiveLock);
}
```