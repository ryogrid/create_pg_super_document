# changeDependencyOnTablespace

## Location
[src/backend/catalog/pg_shdepend.c:391-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L391-L420)

## Overview
Updates shared dependency records when an object's tablespace is changed, ensuring proper tracking of tablespace dependencies in the PostgreSQL system catalog.

## Definition

```c
void
changeDependencyOnTablespace(Oid classId, Oid objectId, Oid newTablespaceId)
```
## Detailed Description
This function manages the update of shared dependency records when a database object's tablespace is modified. It handles two scenarios: when an object is moved to a new tablespace (creating a new dependency) and when an object is moved away from a tablespace (removing the dependency). The function operates on the pg_shdepend system catalog to maintain accurate dependency tracking between database objects and tablespaces.

The function opens the shared dependency relation with exclusive row lock, then either creates a new dependency record via shdepChangeDep() or drops the existing dependency via shdepDropDependency() depending on whether the new tablespace is valid and not the default tablespace.

## Parameters / Member Variables
- : The OID of the system catalog class containing the object (e.g., RelationRelationId for tables)
- : The OID of the specific object whose tablespace dependency is being changed
- : The OID of the new tablespace, or InvalidOid/DEFAULTTABLESPACE_OID if removing tablespace dependency

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [shdepChangeDep](../s/shdepChangeDep.md)
  - [shdepDropDependency](../s/shdepDropDependency.md)  
  - [table_close](../t/table_close.md)
  - SHARED_DEPENDENCY_TABLESPACE
  - SHARED_DEPENDENCY_INVALID
- Called from (representative examples):
  - [SetRelationTableSpace](../S/SetRelationTableSpace.md)

## Notes and Other Information
- Only operates on whole objects (no objsubid parameter needed) since tablespaces apply to entire objects
- Uses RowExclusiveLock on SharedDependRelationId to ensure atomic updates
- Handles default tablespace case by removing dependency records rather than creating them
- Part of the shared dependency management system for tracking cross-database dependencies