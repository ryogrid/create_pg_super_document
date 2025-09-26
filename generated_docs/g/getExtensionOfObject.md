# getExtensionOfObject

## Location
[src/backend/catalog/pg_depend.c:733-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L733-L778)

## Overview
Finds the extension that contains a specified database object, returning the extension's OID or InvalidOid if the object doesn't belong to any extension.

## Definition

```c
Oid
getExtensionOfObject(Oid classId, Oid objectId)
```
## Detailed Description
The `getExtensionOfObject` function searches the `pg_depend` system catalog to determine which extension, if any, contains a given database object. Extension membership is indicated by an EXTENSION dependency relationship from the object to the extension in the dependency catalog.

The function performs a systematic scan of the `pg_depend` table, looking for entries where:
- The `classid` and `objid` match the specified object
- The `refclassid` points to the ExtensionRelationId (indicating the dependency target is an extension)  
- The `deptype` is DEPENDENCY_EXTENSION (indicating extension membership)

When such a dependency is found, the function returns the `refobjid`, which represents the OID of the containing extension. The function stops scanning after finding the first match, under the assumption that objects should belong to at most one extension.

## Parameters / Member Variables
- `classId`: The OID of the system catalog that contains the object (e.g., RelationRelationId for tables)
- `objectId`: The OID of the specific object within that catalog

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_depend
  - DEPENDENCY_EXTENSION
- Called from (representative examples):
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)
  - [pg_extension_config_dump](../p/pg_extension_config_dump.md)
  - [AlterExtensionNamespace](../A/AlterExtensionNamespace.md)
  - [ExecAlterExtensionContentsRecurse](../E/ExecAlterExtensionContentsRecurse.md)

## Notes and Other Information
- Returns InvalidOid if the object is not part of any extension
- The function assumes that objects belong to at most one extension - if multiple extension dependencies exist, the result is indeterminate
- Uses AccessShareLock when accessing the pg_depend catalog to ensure consistent reads
- The DependDependerIndexId index is used to efficiently locate dependency records for the specified object
- This function is essential for extension management operations and dependency tracking in PostgreSQL's extension system