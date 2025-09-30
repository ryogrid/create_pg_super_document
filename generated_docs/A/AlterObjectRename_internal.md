# AlterObjectRename_internal

## Location
[src/backend/commands/alter.c:165-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L165-L356)

## Overview
A generic internal function that renames database objects by updating their name column in the appropriate catalog relation, handling permission checks and duplicate name detection.

## Definition
```c
static void AlterObjectRename_internal(Relation rel, Oid objectId, const char *new_name)
```

## Detailed Description
This function provides a generic mechanism for renaming various types of database objects that can be renamed by simply changing their name column in a single catalog table. It performs comprehensive permission checks, validates ownership, checks for naming conflicts, and updates the catalog entry. The function handles objects with and without namespaces, and includes special logic for specific object types like subscriptions, procedures, collations, operator classes, and operator families. It uses PostgreSQL's catalog cache system for efficient lookups and maintains referential integrity through dependency tracking.

## Parameters / Member Variables
- `rel`: Catalog relation containing the object (must be opened with RowExclusiveLock by caller)
- `objectId`: OID of the object to be renamed
- `new_name`: C string representation of the new name for the object

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid (get relation OID)
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md)/name (cache ID functions)
  - [get_object_attnum_name](../g/get_object_attnum_name.md)/namespace/owner (attribute number functions)
  - [SearchSysCache1](../S/SearchSysCache1.md) (cache lookup)
  - [heap_getattr](../h/heap_getattr.md) (extract attributes from tuples)
  - [superuser](../s/superuser.md) (check superuser privileges)
  - [has_privs_of_role](../h/has_privs_of_role.md) (role privilege checking)
  - [object_aclcheck](../o/object_aclcheck.md) (access control checking)
  - [aclcheck_error](../a/aclcheck_error.md) (ACL error reporting)
  - Various object-specific existence check functions (IsThereFunctionInNamespace, etc.)
  - [report_name_conflict](../r/report_name_conflict.md)/report_namespace_conflict (conflict reporting)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (tuple modification)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog update)
  - InvokeObjectPostAlterHook (post-alter hook)
  - [LogicalRepWorkersWakeupAtCommit](../L/LogicalRepWorkersWakeupAtCommit.md) (subscription-specific)

- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md) (src/backend/commands/alter.c:434)

## Notes and Other Information
- This is a static function, only accessible within src/backend/commands/alter.c
- Designed for simple rename operations where only the name column needs to be updated
- Not suitable for tables or complex objects requiring additional structural changes
- Includes comprehensive permission checking: superuser bypass, ownership verification, and namespace CREATE privileges
- Handles special cases for subscriptions including password_required validation and replication worker notification
- Uses object-specific duplicate name checking functions for procedures, collations, operator classes, and families
- Employs PostgreSQL's heap tuple modification and catalog update mechanisms
- Memory management includes proper cleanup of allocated arrays and tuples
- Supports both namespace-aware and global objects through conditional namespace handling

## Simplified Source

```c
static void
AlterObjectRename_internal(Relation rel, Oid objectId, const char *new_name)
{
    Oid classId = RelationGetRelid(rel);
    int oidCacheId = get_object_catcache_oid(classId);
    int nameCacheId = get_object_catcache_name(classId);
    AttrNumber Anum_name = get_object_attnum_name(classId);
    AttrNumber Anum_namespace = get_object_attnum_namespace(classId);
    AttrNumber Anum_owner = get_object_attnum_owner(classId);
    HeapTuple oldtup, newtup;
    Datum datum;
    bool isnull;
    Oid namespaceId, ownerId;
    char *old_name;
    AclResult aclresult;

    // Look up the object in catalog cache
    oldtup = SearchSysCache1(oidCacheId, ObjectIdGetDatum(objectId));
    if (!HeapTupleIsValid(oldtup))
        elog(ERROR, "cache lookup failed for object %u", objectId);

    // Extract current name and namespace
    datum = heap_getattr(oldtup, Anum_name, RelationGetDescr(rel), &isnull);
    old_name = NameStr(*(DatumGetName(datum)));

    if (Anum_namespace > 0) {
        datum = heap_getattr(oldtup, Anum_namespace, RelationGetDescr(rel), &isnull);
        namespaceId = DatumGetObjectId(datum);
    } else {
        namespaceId = InvalidOid;
    }

    // Permission checks
    if (!superuser()) {
        // Must have an owner attribute
        if (Anum_owner <= 0)
            ereport(ERROR, "must be superuser to rename this object");

        // Check ownership
        datum = heap_getattr(oldtup, Anum_owner, RelationGetDescr(rel), &isnull);
        ownerId = DatumGetObjectId(datum);
        if (!has_privs_of_role(GetUserId(), ownerId))
            aclcheck_error(ACLCHECK_NOT_OWNER, get_object_type(classId, objectId), old_name);

        // Check CREATE privilege on namespace
        if (OidIsValid(namespaceId)) {
            aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceId));
        }

        // Special subscription checks
        if (classId == SubscriptionRelationId) {
            Form_pg_subscription form = (Form_pg_subscription) GETSTRUCT(oldtup);

            // Need CREATE on database
            aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(MyDatabaseId));

            // Check password_required setting
            if (!form->subpasswordrequired && !superuser())
                ereport(ERROR, "password_required=false is superuser-only");
        }
    }

    // Check for duplicate names (object-specific logic)
    if (classId == ProcedureRelationId) {
        Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(oldtup);
        IsThereFunctionInNamespace(new_name, proc->pronargs, &proc->proargtypes, proc->pronamespace);
    } else if (classId == CollationRelationId) {
        Form_pg_collation coll = (Form_pg_collation) GETSTRUCT(oldtup);
        IsThereCollationInNamespace(new_name, coll->collnamespace);
    } else if (classId == SubscriptionRelationId) {
        if (SearchSysCacheExists2(SUBSCRIPTIONNAME, ObjectIdGetDatum(MyDatabaseId), CStringGetDatum(new_name)))
            report_name_conflict(classId, new_name);
        // Wake up replication workers
        LogicalRepWorkersWakeupAtCommit(objectId);
    } else if (nameCacheId >= 0) {
        // Generic name conflict check
        if (OidIsValid(namespaceId)) {
            if (SearchSysCacheExists2(nameCacheId, CStringGetDatum(new_name), ObjectIdGetDatum(namespaceId)))
                report_namespace_conflict(classId, new_name, namespaceId);
        } else {
            if (SearchSysCacheExists1(nameCacheId, CStringGetDatum(new_name)))
                report_name_conflict(classId, new_name);
        }
    }

    // Update the catalog entry
    Datum *values = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(Datum));
    bool *nulls = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    bool *replaces = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));

    NameData nameattrdata;
    namestrcpy(&nameattrdata, new_name);
    values[Anum_name - 1] = NameGetDatum(&nameattrdata);
    replaces[Anum_name - 1] = true;

    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);
    CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

    // Cleanup
    InvokeObjectPostAlterHook(classId, objectId, 0);
    pfree(values);
    pfree(nulls);
    pfree(replaces);
    heap_freetuple(newtup);
    ReleaseSysCache(oldtup);
}
```