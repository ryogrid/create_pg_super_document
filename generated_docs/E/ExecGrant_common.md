# ExecGrant_common

## Location
[src/backend/catalog/aclchk.c:2156-2291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2156-L2291)

## Overview
ExecGrant_common is the core implementation function for processing GRANT and REVOKE statements on database objects, handling the common ACL modification logic across different object types.

## Definition

```c
static void
ExecGrant_common(InternalGrant *istmt, Oid classid, AclMode default_privs,
				 void (*object_check) (InternalGrant *istmt, HeapTuple tuple))
```
## Detailed Description
ExecGrant_common performs the core work of granting or revoking privileges on database objects. It iterates through each object specified in the GRANT/REVOKE statement, retrieves the current ACL, applies the privilege changes using merge_acl_with_grant, and updates the system catalogs. The function handles privilege validation, grantor selection, and maintains dependency tracking for proper cleanup when roles are dropped.

The function is designed to work with any catalog table that stores ACLs by accepting a classid parameter and optional object-specific validation callback. It ensures atomicity by using appropriate locking and handles duplicate objects gracefully.

## Parameters / Member Variables
- `*istmt`: Internal representation of the GRANT/REVOKE statement containing grantees, privileges, and options
- `classid`: OID of the system catalog class (e.g., RelationRelationId for tables)
- `default_privs`: Default privileges to grant when ALL PRIVILEGES is specified
- `*object_check`: Optional callback function for object-type-specific validation
## Dependencies
- Functions called/Symbols referenced:
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md)
  - [table_open](../t/table_open.md), table_close
  - [SearchSysCacheLocked1](../S/SearchSysCacheLocked1.md), ReleaseSysCache
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md), SysCacheGetAttrNotNull
  - [acldefault](../a/acldefault.md), aclmembers
  - [select_best_grantor](../s/select_best_grantor.md)
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md), CatalogTupleUpdate
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - [recordExtensionInitPriv](../r/recordExtensionInitPriv.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from:
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md) (for various object types like tables, functions, databases, etc.)

## Notes and Other Information
- Uses row-exclusive locking on the target catalog to prevent concurrent modifications
- Handles both explicit privilege lists and ALL PRIVILEGES grants
- Maintains shared dependency records to track which roles have privileges on objects
- Records extension privileges for proper pg_dump/restore handling
- Increments command counter after each object to handle duplicate processing
- The object_check callback allows type-specific validation (e.g., checking language trust for procedural languages)

## Simplified Source

```c
static void
ExecGrant_common(InternalGrant *istmt, Oid classid, AclMode default_privs,
                 void (*object_check) (InternalGrant *istmt, HeapTuple tuple))
{
    int cacheid;
    Relation relation;
    ListCell *cell;

    // Set default privileges if ALL PRIVILEGES specified
    if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
        istmt->privileges = default_privs;

    // Get cache ID and open catalog table
    cacheid = get_object_catcache_oid(classid);
    relation = table_open(classid, RowExclusiveLock);

    // Process each object in the grant/revoke statement
    foreach(cell, istmt->objects)
    {
        Oid objectid = lfirst_oid(cell);
        Acl *old_acl, *new_acl;
        Oid grantorId, ownerId;
        AclMode avail_goptions, this_privileges;
        HeapTuple tuple, newtuple;
        bool isNull;

        // Look up object in catalog
        tuple = SearchSysCacheLocked1(cacheid, ObjectIdGetDatum(objectid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for %s %u",
                 get_object_class_descr(classid), objectid);

        // Perform object-specific validation if needed
        if (object_check)
            object_check(istmt, tuple);

        // Get object owner and current ACL
        ownerId = DatumGetObjectId(SysCacheGetAttrNotNull(cacheid, tuple,
                                   get_object_attnum_owner(classid)));

        // Get existing ACL or use default
        Datum aclDatum = SysCacheGetAttr(cacheid, tuple,
                                       get_object_attnum_acl(classid), &isNull);
        if (isNull)
            old_acl = acldefault(get_object_type(classid, objectid), ownerId);
        else
            old_acl = DatumGetAclPCopy(aclDatum);

        // Select best grantor and available grant options
        select_best_grantor(GetUserId(), istmt->privileges,
                           old_acl, ownerId, &grantorId, &avail_goptions);

        // Validate and restrict privileges
        this_privileges = restrict_and_check_grant(istmt->is_grant, avail_goptions,
                                                  istmt->all_privs, istmt->privileges,
                                                  objectid, grantorId,
                                                  get_object_type(classid, objectid),
                                                  "object_name", 0, NULL);

        // Generate new ACL by merging with grant/revoke
        new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
                                      istmt->grant_option, istmt->behavior,
                                      istmt->grantees, this_privileges,
                                      grantorId, ownerId);

        // Update catalog with new ACL
        Datum *values = palloc0_array(Datum, RelationGetDescr(relation)->natts);
        bool *nulls = palloc0_array(bool, RelationGetDescr(relation)->natts);
        bool *replaces = palloc0_array(bool, RelationGetDescr(relation)->natts);

        values[get_object_attnum_acl(classid) - 1] = PointerGetDatum(new_acl);
        replaces[get_object_attnum_acl(classid) - 1] = true;

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation),
                                    values, nulls, replaces);
        CatalogTupleUpdate(relation, &newtuple->t_self, newtuple);

        // Update extension privileges and dependencies
        recordExtensionInitPriv(objectid, classid, 0, new_acl);

        // Update ACL dependencies for role tracking
        int old_members_count, new_members_count;
        Oid *old_members, *new_members;
        old_members_count = aclmembers(old_acl, &old_members);
        new_members_count = aclmembers(new_acl, &new_members);
        updateAclDependencies(classid, objectid, 0, ownerId,
                             old_members_count, old_members,
                             new_members_count, new_members);

        ReleaseSysCache(tuple);
        CommandCounterIncrement();
    }

    table_close(relation, RowExclusiveLock);
}
```