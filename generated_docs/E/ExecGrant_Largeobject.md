# ExecGrant_Largeobject

## Location
[src/backend/catalog/aclchk.c:2308-2443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2308-L2443)

## Overview
ExecGrant_Largeobject handles GRANT and REVOKE operations specifically for large objects, which require special handling due to their unique catalog structure.

## Definition

```c
static void
ExecGrant_Largeobject(InternalGrant *istmt)
```
## Detailed Description
ExecGrant_Largeobject implements privilege management for PostgreSQL large objects (LOBs). Unlike other database objects that use syscache for catalog access, large objects require direct table scanning of pg_largeobject_metadata since there's no syscache available. The function follows the same general pattern as ExecGrant_common but with large-object-specific catalog access methods.

Large objects have their own privilege set (SELECT and UPDATE privileges) defined by ACL_ALL_RIGHTS_LARGEOBJECT. The function handles ACL modification, dependency tracking, and extension privilege recording specifically for the large object subsystem.

## Parameters / Member Variables
- `*istmt`: Internal representation of the GRANT/REVOKE statement containing target large object OIDs, grantees, privileges, and options
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_close (with LargeObjectMetadataRelationId)
  - [ScanKeyInit](../S/ScanKeyInit.md), systable_beginscan, systable_getnext, systable_endscan
  - [heap_getattr](../h/heap_getattr.md), heap_modify_tuple
  - [acldefault](../a/acldefault.md), aclmembers (with OBJECT_LARGEOBJECT)
  - [select_best_grantor](../s/select_best_grantor.md)
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [updateAclDependencies](../u/updateAclDependencies.md) (with LargeObjectRelationId)
  - [recordExtensionInitPriv](../r/recordExtensionInitPriv.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from:
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md) (when processing large object privileges)

## Notes and Other Information
- Cannot use ExecGrant_common because pg_largeobject_metadata lacks syscache support
- Uses systable_beginscan with LargeObjectMetadataOidIndexId for efficient lookup
- Handles ACL_ALL_RIGHTS_LARGEOBJECT as the default privilege set for ALL PRIVILEGES
- Creates readable names like "large object 12345" for error messages
- Updates dependencies using LargeObjectRelationId rather than LargeObjectMetadataRelationId
- Large objects support SELECT (read) and UPDATE (write) privileges

## Simplified Source

```c
static void
ExecGrant_Largeobject(InternalGrant *istmt)
{
    Relation relation;
    ListCell *cell;

    // Set default privileges if ALL PRIVILEGES specified
    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
        istmt->privileges = ACL_ALL_RIGHTS_LARGEOBJECT;

    // Open large object metadata catalog
    relation = table_open(LargeObjectMetadataRelationId, RowExclusiveLock);

    // Process each large object
    foreach(cell, istmt->objects)
    {
        Oid loid = lfirst_oid(cell);
        Form_pg_largeobject_metadata form_lo_meta;
        char loname[NAMEDATALEN];
        Datum aclDatum;
        bool isNull;
        AclMode avail_goptions, this_privileges;
        Acl *old_acl, *new_acl;
        Oid grantorId, ownerId;
        HeapTuple tuple, newtuple;
        // ... variable declarations for ACL management ...

        // Find large object metadata tuple
        ScanKeyInit(&entry[0], Anum_pg_largeobject_metadata_oid,
                    BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(loid));
        scan = systable_beginscan(relation, LargeObjectMetadataOidIndexId,
                                  true, NULL, 1, entry);
        tuple = systable_getnext(scan);
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "could not find tuple for large object %u", loid);

        form_lo_meta = (Form_pg_largeobject_metadata) GETSTRUCT(tuple);
        ownerId = form_lo_meta->lomowner;

        // Get existing ACL or use default
        aclDatum = heap_getattr(tuple, Anum_pg_largeobject_metadata_lomacl,
                                RelationGetDescr(relation), &isNull);
        if (isNull)
            old_acl = acldefault(OBJECT_LARGEOBJECT, ownerId);
        else
            old_acl = DatumGetAclPCopy(aclDatum);

        // Determine grantor and available grant options
        select_best_grantor(GetUserId(), istmt->privileges, old_acl, ownerId,
                            &grantorId, &avail_goptions);

        // Validate and restrict privileges
        snprintf(loname, sizeof(loname), "large object %u", loid);
        this_privileges = restrict_and_check_grant(istmt->is_grant, avail_goptions,
                                                   istmt->all_privs, istmt->privileges,
                                                   loid, grantorId, OBJECT_LARGEOBJECT,
                                                   loname, 0, NULL);

        // Generate new ACL
        new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
                                       istmt->grant_option, istmt->behavior,
                                       istmt->grantees, this_privileges,
                                       grantorId, ownerId);

        // Update catalog with new ACL
        replaces[Anum_pg_largeobject_metadata_lomacl - 1] = true;
        values[Anum_pg_largeobject_metadata_lomacl - 1] = PointerGetDatum(new_acl);
        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation),
                                     values, nulls, replaces);
        CatalogTupleUpdate(relation, &newtuple->t_self, newtuple);

        // Update dependencies and extension privileges
        recordExtensionInitPriv(loid, LargeObjectRelationId, 0, new_acl);
        updateAclDependencies(LargeObjectRelationId, form_lo_meta->oid, 0,
                              ownerId, noldmembers, oldmembers,
                              nnewmembers, newmembers);

        systable_endscan(scan);
        pfree(new_acl);
        CommandCounterIncrement();
    }

    table_close(relation, RowExclusiveLock);
}
```