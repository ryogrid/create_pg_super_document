# CreateOpFamily

## Location
src/backend/commands/opclasscmds.c: 243 - 332

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