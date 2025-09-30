# AlterObjectNamespace_internal

## Location
[src/backend/commands/alter.c:681-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/alter.c#L681-L825)

## Overview
A generic internal function that changes the namespace of a database object by updating its catalog entry, handling permissions, duplicate checks, and dependency updates.

## Definition

```c
static Oid
AlterObjectNamespace_internal(Relation rel, Oid objid, Oid nspOid)
```
## Detailed Description
AlterObjectNamespace_internal provides the core implementation for moving database objects between schemas. This static function handles the common case where namespace alteration only requires updating a single catalog entry's namespace column. It performs comprehensive validation including permission checks, duplicate name detection, and dependency management.

The function operates through several key phases:
1. **Object lookup**: Retrieves the object from the appropriate system cache
2. **Early exit optimization**: Returns immediately if object is already in target namespace
3. **Permission validation**: Ensures user has proper privileges (ownership + CREATE on target schema)
4. **Duplicate detection**: Checks for naming conflicts using type-specific validation functions
5. **Catalog update**: Modifies the namespace column in the catalog tuple
6. **Dependency update**: Updates the dependency system to reflect the new schema relationship

The function includes specialized duplicate checking for functions, collations, operator classes, and operator families, with a generic fallback for other object types.

## Parameters / Member Variables
- : Catalog relation containing the object (must be opened with RowExclusiveLock by caller)
- : OID of the object whose namespace should be changed
- : OID of the target namespace/schema

## Dependencies
- Functions called/Symbols referenced:
  - : Gets system cache ID for object lookups by OID
  - : Gets system cache ID for object lookups by name
  - : Gets attribute numbers for name, namespace, and owner columns
  - : Looks up object tuple in system cache
  - : Extracts attribute values from catalog tuples
  - : Validates namespace change is allowed
  - : Checks if current user is superuser
  - : Validates ownership privileges
  - : Checks CREATE privilege on target namespace
  - : Type-specific duplicate name checking functions
  - : Reports duplicate name errors
  - : Creates modified catalog tuple
  - : Performs the actual catalog update
  - : Updates dependency records
  - : Fires post-alteration hooks
- Called from (representative examples):
  - : Main ALTER OBJECT SET SCHEMA execution
  - : Extension-related namespace changes

## Notes and Other Information
- Returns the OID of the object's previous namespace
- Designed for simple cases - won't work for complex objects like tables that require additional processing
- Includes optimization to avoid unnecessary work when object is already in the correct namespace
- Performs comprehensive permission checking unless the user is a superuser
- Uses type-specific duplicate detection for certain object types (functions, collations, operator classes/families)
- Updates both the catalog tuple and the dependency system atomically
- Static function - only used within the alter.c compilation unit
- Assumes the caller has already acquired appropriate locks on the catalog relation
- Memory management includes proper cleanup of allocated arrays for tuple modification

## Simplified Source

```c
static Oid
AlterObjectNamespace_internal(Relation rel, Oid objid, Oid nspOid)
{
    // Get object metadata from catalog relation
    Oid classId = RelationGetRelid(rel);
    int oidCacheId = get_object_catcache_oid(classId);
    AttrNumber Anum_namespace = get_object_attnum_namespace(classId);

    // Look up the object tuple
    HeapTuple tup = SearchSysCacheCopy1(oidCacheId, ObjectIdGetDatum(objid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for object %u", objid);

    // Get current namespace
    Datum namespace = heap_getattr(tup, Anum_namespace, RelationGetDescr(rel), &isnull);
    Oid oldNspOid = DatumGetObjectId(namespace);

    // If already in target namespace, just fire hook and return
    if (oldNspOid == nspOid) {
        InvokeObjectPostAlterHook(classId, objid, 0);
        return oldNspOid;
    }

    // Basic namespace validation
    CheckSetNamespace(oldNspOid, nspOid);

    // Permission checks (unless superuser)
    if (!superuser()) {
        // Must own the object
        Datum owner = heap_getattr(tup, Anum_owner, RelationGetDescr(rel), &isnull);
        Oid ownerId = DatumGetObjectId(owner);
        if (!has_privs_of_role(GetUserId(), ownerId))
            aclcheck_error(ACLCHECK_NOT_OWNER, get_object_type(classId, objid), name);

        // Must have CREATE privilege on target namespace
        AclResult aclresult = object_aclcheck(NamespaceRelationId, nspOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(nspOid));
    }

    // Check for duplicate names in target namespace
    Datum name = heap_getattr(tup, Anum_name, RelationGetDescr(rel), &isnull);

    // Type-specific duplicate checking
    if (classId == ProcedureRelationId) {
        Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(tup);
        IsThereFunctionInNamespace(NameStr(proc->proname), proc->pronargs,
                                 &proc->proargtypes, nspOid);
    } else if (classId == CollationRelationId) {
        Form_pg_collation coll = (Form_pg_collation) GETSTRUCT(tup);
        IsThereCollationInNamespace(NameStr(coll->collname), nspOid);
    }
    // ... other type-specific checks ...
    else if (nameCacheId >= 0 && SearchSysCacheExists2(nameCacheId, name, ObjectIdGetDatum(nspOid))) {
        report_namespace_conflict(classId, NameStr(*(DatumGetName(name))), nspOid);
    }

    // Update catalog tuple with new namespace
    Datum *values = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(Datum));
    bool *nulls = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));
    bool *replaces = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(bool));

    values[Anum_namespace - 1] = ObjectIdGetDatum(nspOid);
    replaces[Anum_namespace - 1] = true;

    HeapTuple newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
    CatalogTupleUpdate(rel, &tup->t_self, newtup);

    // Update dependency record
    changeDependencyFor(classId, objid, NamespaceRelationId, oldNspOid, nspOid);

    // Cleanup and fire post-alter hook
    pfree(values);
    pfree(nulls);
    pfree(replaces);
    InvokeObjectPostAlterHook(classId, objid, 0);

    return oldNspOid;
}
```