# AlterTypeNamespaceInternal

## Location
[src/backend/commands/typecmds.c:4156-4311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L4156-L4311)

## Overview
The core internal function that performs the actual namespace migration for PostgreSQL types, handling all type variants including composite types, domains, and arrays with comprehensive dependency management.

## Definition

```c
Oid
AlterTypeNamespaceInternal(Oid typeOid, Oid nspOid,
						   bool isImplicitArray,
						   bool ignoreDependent,
						   bool errorOnTableType,
						   ObjectAddresses *objsMoved)
```
## Detailed Description
AlterTypeNamespaceInternal is the workhorse function that performs the actual type namespace change operations. It handles the complete process including catalog updates, dependency tracking, constraint migration, and recursive processing of associated array types. The function distinguishes between different type categories (composite types, domains, table row types) and applies appropriate handling for each. It maintains referential integrity by updating both pg_type and pg_class catalogs for composite types and properly managing namespace dependencies.

## Parameters / Member Variables
- : OID of the type to be moved to the new namespace
- : OID of the target namespace where the type should be relocated
- : Boolean flag indicating if this is an internal recursive call for an array type
- : Boolean flag to silently skip table row types instead of erroring
- : Boolean flag to raise an error when encountering table row types (ignored if ignoreDependent is true)
- : ObjectAddresses structure tracking all objects moved during the operation to prevent duplicate processing

## Dependencies
- Functions called/Symbols referenced:
  - [object_address_present](../o/object_address_present.md)
  - SearchSysCacheCopy1
  - [CheckSetNamespace](../C/CheckSetNamespace.md)
  - SearchSysCacheExists2
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [AlterRelationNamespaceInternal](AlterRelationNamespaceInternal.md)
  - [AlterConstraintNamespaces](AlterConstraintNamespaces.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
- Called from (representative examples):
  - [AlterTypeNamespace_oid](AlterTypeNamespace_oid.md)
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md)
  - [AlterTypeNamespaceInternal](AlterTypeNamespaceInternal.md) (recursive call)

## Notes and Other Information
- Automatically recurses to process associated array types when moving a base type
- Prevents duplicate processing by checking objsMoved before starting work
- Handles composite types by updating both pg_type and pg_class catalogs
- Migrates constraints for both composite types and domain types to the new namespace
- Updates schema dependencies except for table row types and implicit arrays
- Returns InvalidOid if no action was taken, otherwise returns the old namespace OID
- Invokes post-alter hooks to notify other subsystems of the change

## Simplified Source

```c
Oid
AlterTypeNamespaceInternal(Oid typeOid, Oid nspOid,
                          bool isImplicitArray,
                          bool ignoreDependent,
                          bool errorOnTableType,
                          ObjectAddresses *objsMoved)
{
    Relation    rel;
    HeapTuple   tup;
    Form_pg_type typform;
    Oid         oldNspOid;
    Oid         arrayOid;
    bool        isCompositeType;
    ObjectAddress thisobj;

    // Check if already moved
    thisobj.classId = TypeRelationId;
    thisobj.objectId = typeOid;
    thisobj.objectSubId = 0;

    if (object_address_present(&thisobj, objsMoved))
        return InvalidOid;

    // Open pg_type and get the type tuple
    rel = table_open(TypeRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", typeOid);

    typform = (Form_pg_type) GETSTRUCT(tup);
    oldNspOid = typform->typnamespace;
    arrayOid = typform->typarray;

    // Check namespace constraints if actually moving
    if (oldNspOid != nspOid)
    {
        CheckSetNamespace(oldNspOid, nspOid);

        // Check for name conflicts
        if (SearchSysCacheExists2(TYPENAMENSP,
                                  NameGetDatum(&typform->typname),
                                  ObjectIdGetDatum(nspOid)))
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                     errmsg("type \"%s\" already exists in schema \"%s\"",
                            NameStr(typform->typname),
                            get_namespace_name(nspOid))));
    }

    // Determine if this is a composite type (not table rowtype)
    isCompositeType = (typform->typtype == TYPTYPE_COMPOSITE &&
                       get_rel_relkind(typform->typrelid) == RELKIND_COMPOSITE_TYPE);

    // Handle table row types based on flags
    if (typform->typtype == TYPTYPE_COMPOSITE && !isCompositeType)
    {
        if (ignoreDependent)
        {
            table_close(rel, RowExclusiveLock);
            return InvalidOid;
        }
        if (errorOnTableType)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s is a table's row type", format_type_be(typeOid)),
                     errhint("Use %s instead.", "ALTER TABLE")));
    }

    // Update pg_type if namespace is changing
    if (oldNspOid != nspOid)
    {
        typform->typnamespace = nspOid;
        CatalogTupleUpdate(rel, &tup->t_self, tup);
    }

    // Handle composite types: update pg_class and constraints
    if (isCompositeType)
    {
        Relation classRel = table_open(RelationRelationId, RowExclusiveLock);
        AlterRelationNamespaceInternal(classRel, typform->typrelid,
                                       oldNspOid, nspOid, false, objsMoved);
        table_close(classRel, RowExclusiveLock);

        AlterConstraintNamespaces(typform->typrelid, oldNspOid,
                                  nspOid, false, objsMoved);
    }
    else if (typform->typtype == TYPTYPE_DOMAIN)
    {
        // Handle domain constraints
        AlterConstraintNamespaces(typeOid, oldNspOid, nspOid, true, objsMoved);
    }

    // Update schema dependency (except for table rowtypes and implicit arrays)
    if (oldNspOid != nspOid &&
        (isCompositeType || typform->typtype != TYPTYPE_COMPOSITE) &&
        !isImplicitArray)
    {
        if (changeDependencyFor(TypeRelationId, typeOid,
                                NamespaceRelationId, oldNspOid, nspOid) != 1)
            elog(ERROR, "could not change schema dependency for type \"%s\"",
                 format_type_be(typeOid));
    }

    // Cleanup and finalize
    InvokeObjectPostAlterHook(TypeRelationId, typeOid, 0);
    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);
    add_exact_object_address(&thisobj, objsMoved);

    // Recursively process array type if it exists
    if (OidIsValid(arrayOid))
        AlterTypeNamespaceInternal(arrayOid, nspOid, true, false, true, objsMoved);

    return oldNspOid;
}
```