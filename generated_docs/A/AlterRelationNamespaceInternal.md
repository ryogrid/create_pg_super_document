# AlterRelationNamespaceInternal

## Location
[src/backend/commands/tablecmds.c:17315-17391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17315-L17391)

## Overview
A core internal function that relocates a relation (table, index, sequence, etc.) from one namespace (schema) to another by updating the pg_class catalog entry and related dependency information.

## Definition

```c
void
AlterRelationNamespaceInternal(Relation classRel, Oid relOid,
							   Oid oldNspOid, Oid newNspOid,
							   bool hasDependEntry,
							   ObjectAddresses *objsMoved)
```
## Detailed Description
This function implements the core logic for moving a relation between namespaces. It operates on the pg_class catalog directly, updating the relnamespace field and managing associated dependencies. The function includes safeguards against duplicate relation names in the target namespace and tracks moved objects to prevent duplicate operations. It requires the caller to have already opened and write-locked the pg_class relation for thread safety.

## Parameters / Member Variables
- `classRel`: Pre-opened and write-locked pg_class relation for catalog updates
- `relOid`: Object identifier of the relation being moved
- `oldNspOid`: Object identifier of the source namespace
- `newNspOid`: Object identifier of the destination namespace
- `hasDependEntry`: Boolean indicating whether to update schema dependency entries
- `*objsMoved`: Collection tracking objects already processed to prevent duplicates
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md)
  - [object_address_present](../o/object_address_present.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md)
  - [AlterIndexNamespaces](AlterIndexNamespaces.md)
  - [AlterSeqNamespaces](AlterSeqNamespaces.md)
  - [AlterTypeNamespaceInternal](AlterTypeNamespaceInternal.md)

## Notes and Other Information
- Checks for name conflicts in the target namespace before proceeding with the move
- Uses tuple locking mechanisms to ensure data consistency during catalog updates
- Fires post-alter hooks to notify other subsystems of the namespace change
- Handles cases where objects have already been moved or are already in the correct namespace
- Critical for implementing ALTER TABLE/INDEX/SEQUENCE SET SCHEMA operations

## Simplified Source

```c
void
AlterRelationNamespaceInternal(Relation classRel, Oid relOid,
                              Oid oldNspOid, Oid newNspOid,
                              bool hasDependEntry,
                              ObjectAddresses *objsMoved)
{
    HeapTuple   classTup;
    Form_pg_class classForm;
    ObjectAddress thisobj;
    bool        already_done = false;

    // Lock and get the relation's catalog entry
    classTup = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(classTup))
        elog(ERROR, "cache lookup failed for relation %u", relOid);

    classForm = (Form_pg_class) GETSTRUCT(classTup);
    Assert(classForm->relnamespace == oldNspOid);

    // Setup object address
    thisobj.classId = RelationRelationId;
    thisobj.objectId = relOid;
    thisobj.objectSubId = 0;

    // Check if already moved
    already_done = object_address_present(&thisobj, objsMoved);

    if (!already_done && oldNspOid != newNspOid)
    {
        // Check for name conflicts in target namespace
        if (get_relname_relid(NameStr(classForm->relname), newNspOid) != InvalidOid)
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_TABLE),
                     errmsg("relation \"%s\" already exists in schema \"%s\"",
                            NameStr(classForm->relname),
                            get_namespace_name(newNspOid))));

        // Update the namespace in pg_class
        classForm->relnamespace = newNspOid;
        CatalogTupleUpdate(classRel, &classTup->t_self, classTup);
        UnlockTuple(classRel, &classTup->t_self, InplaceUpdateTupleLock);

        // Update schema dependency if needed
        if (hasDependEntry &&
            changeDependencyFor(RelationRelationId, relOid,
                               NamespaceRelationId,
                               oldNspOid, newNspOid) != 1)
            elog(ERROR, "could not change schema dependency for relation \"%s\"",
                 NameStr(classForm->relname));
    }
    else
        UnlockTuple(classRel, &classTup->t_self, InplaceUpdateTupleLock);

    if (!already_done)
    {
        add_exact_object_address(&thisobj, objsMoved);
        InvokeObjectPostAlterHook(RelationRelationId, relOid, 0);
    }

    heap_freetuple(classTup);
}
```