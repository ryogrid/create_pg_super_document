# CreateOpFamily

## Location
[src/backend/commands/opclasscmds.c:243-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L243-L332)

## Overview
CreateOpFamily is a static function that creates a new operator family entry in the PostgreSQL system catalog, handling all necessary catalog updates and dependency management.

## Definition
```c
static ObjectAddress CreateOpFamily(CreateOpFamilyStmt *stmt, const char *opfname, Oid namespaceoid, Oid amoid)
```

## Detailed Description
This function performs the complete process of creating a new operator family in the PostgreSQL system. It handles catalog entry creation, uniqueness validation, dependency establishment, and event notification. The function ensures data integrity by checking for naming conflicts before creation and establishes proper dependencies to maintain referential integrity within the system catalog.

The function follows PostgreSQL's standard pattern for catalog object creation: validating input, creating the catalog entry, establishing dependencies, and notifying interested subsystems about the creation. It also integrates with PostgreSQL's extension system and event trigger mechanism.

## Parameters
- `stmt`: The CREATE OPERATOR FAMILY statement containing creation parameters and metadata
- `opfname`: The name of the operator family to create
- `namespaceoid`: The OID of the namespace (schema) where the operator family will be created
- `amoid`: The OID of the access method that this operator family will support

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists3
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - [EventTriggerCollectSimpleCommand](../E/EventTriggerCollectSimpleCommand.md)
  - InvokeObjectPostCreateHook
- Called from:
  - [DefineOpClass](../D/DefineOpClass.md)
  - [DefineOpFamily](../D/DefineOpFamily.md)

## Notes and Other Information
- This is a static function accessible only within opclasscmds.c
- Performs comprehensive dependency management including dependencies on access method, namespace, owner, and current extension
- Integrates with PostgreSQL's event trigger system for DDL auditing and monitoring
- Uses proper locking (RowExclusiveLock) to ensure concurrent safety during catalog modifications
- Returns an ObjectAddress structure that can be used for further operations on the created operator family
- The function ensures ACID properties by properly managing the catalog transaction and error handling

## Simplified Source

```c
static ObjectAddress
CreateOpFamily(CreateOpFamilyStmt *stmt, const char *opfname,
               Oid namespaceoid, Oid amoid)
{
    Oid opfamilyoid;
    Relation rel;
    HeapTuple tup;
    Datum values[Natts_pg_opfamily];
    bool nulls[Natts_pg_opfamily];
    NameData opfName;
    ObjectAddress myself, referenced;

    rel = table_open(OperatorFamilyRelationId, RowExclusiveLock);

    // Check for existing operator family with same name
    if (SearchSysCacheExists3(OPFAMILYAMNAMENSP,
                              ObjectIdGetDatum(amoid),
                              CStringGetDatum(opfname),
                              ObjectIdGetDatum(namespaceoid)))
        ereport(ERROR, "operator family already exists");

    // Create pg_opfamily entry
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    opfamilyoid = GetNewOidWithIndex(rel, OpfamilyOidIndexId, Anum_pg_opfamily_oid);
    values[Anum_pg_opfamily_oid - 1] = ObjectIdGetDatum(opfamilyoid);
    values[Anum_pg_opfamily_opfmethod - 1] = ObjectIdGetDatum(amoid);
    namestrcpy(&opfName, opfname);
    values[Anum_pg_opfamily_opfname - 1] = NameGetDatum(&opfName);
    values[Anum_pg_opfamily_opfnamespace - 1] = ObjectIdGetDatum(namespaceoid);
    values[Anum_pg_opfamily_opfowner - 1] = ObjectIdGetDatum(GetUserId());

    tup = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    // Create dependencies
    myself.classId = OperatorFamilyRelationId;
    myself.objectId = opfamilyoid;
    myself.objectSubId = 0;

    // Depend on access method
    referenced.classId = AccessMethodRelationId;
    referenced.objectId = amoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    // Depend on namespace
    referenced.classId = NamespaceRelationId;
    referenced.objectId = namespaceoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    // Record ownership and extension dependencies
    recordDependencyOnOwner(OperatorFamilyRelationId, opfamilyoid, GetUserId());
    recordDependencyOnCurrentExtension(&myself, false);

    // Notify event triggers and invoke creation hooks
    EventTriggerCollectSimpleCommand(myself, InvalidObjectAddress, (Node *) stmt);
    InvokeObjectPostCreateHook(OperatorFamilyRelationId, opfamilyoid, 0);

    table_close(rel, RowExclusiveLock);
    return myself;
}
```