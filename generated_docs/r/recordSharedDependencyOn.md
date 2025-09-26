# recordSharedDependencyOn

## Location
[src/backend/catalog/pg_shdepend.c:125-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L125-L167)

## Overview
Records a dependency relationship between two objects where the referenced object is a shared object (accessible across multiple databases) in the PostgreSQL system catalog.

## Definition

```c
void
recordSharedDependencyOn(ObjectAddress *depender,
						 ObjectAddress *referenced,
						 SharedDependencyType deptype)
```
## Detailed Description
This function creates an entry in the pg_shdepend catalog table to track dependencies between database objects where the referenced object is shared across databases (like users, roles, tablespaces). It ensures the referenced object still exists by locking it, then records the dependency relationship. The lock is maintained until the end of the current transaction. Dependencies on pinned objects (system objects that cannot be dropped) are not recorded as they are considered permanent.

## Parameters / Member Variables
- : Pointer to ObjectAddress of the dependent object that relies on the referenced object
- : Pointer to ObjectAddress of the referenced shared object (must be a shared object)
- : Type of shared dependency relationship (SharedDependencyType enum value)

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - [table_open](../t/table_open.md)
  - [IsPinnedObject](../I/IsPinnedObject.md)
  - [shdepAddDependency](../s/shdepAddDependency.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [recordDependencyOnOwner](recordDependencyOnOwner.md)
  - [recordDependencyOnTablespace](recordDependencyOnTablespace.md)
  - [CreatePolicy](../C/CreatePolicy.md)
  - [AlterPolicy](../A/AlterPolicy.md)
  - [RemoveRoleFromObjectPolicy](../R/RemoveRoleFromObjectPolicy.md)

## Notes and Other Information
- Objects in pg_shdepend cannot have SubIds (must be 0)
- During bootstrap mode, no dependencies are recorded as pg_shdepend may not exist yet
- Pinned objects are excluded from dependency tracking as they are permanent system objects
- The function opens pg_shdepend with RowExclusiveLock to ensure consistency during dependency recording
- Located in src/backend/catalog/pg_shdepend.c:125-167