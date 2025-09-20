# IsPinnedObject

## Location
[src/backend/catalog/catalog.c:343-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L343-L420)

## Overview
IsPinnedObject determines whether a database object is "pinned" and therefore cannot be dropped because the system requires it to function properly.

## Definition

```c
bool
IsPinnedObject(Oid classId, Oid objectId)
```
## Detailed Description
This function identifies system objects that are essential to PostgreSQL's operation and cannot be dropped by users. Instead of maintaining explicit dependency records in pg_depend (which proved to be expensive overhead), it uses an efficient OID range test combined with specific class-based exceptions. Objects with OIDs below FirstUnpinnedObjectId are generally considered pinned, representing initdb-created system objects. The function includes several policy-based exceptions, such as large objects (which can have user-assigned OIDs), the public namespace, and databases (including templates), which are deliberately not pinned to allow certain administrative operations.

## Parameters / Member Variables
- : The OID of the system catalog (pg_class entry) that contains the object
- : The OID of the specific object to check

## Dependencies
- Functions called/Symbols referenced:
  - FirstUnpinnedObjectId (constant defining the boundary between system and user objects)
- Called from (representative examples):
  - [findDependentObjects](../f/findDependentObjects.md) (src/backend/catalog/dependency.c:494)
  - [isObjectPinned](../i/isObjectPinned.md) (src/backend/catalog/pg_depend.c:712)
  - [recordSharedDependencyOn](../r/recordSharedDependencyOn.md) (src/backend/catalog/pg_shdepend.c:147)
  - [checkSharedDependencies](../c/checkSharedDependencies.md) (src/backend/catalog/pg_shdepend.c:696)
  - [shdepDropOwned](../s/shdepDropOwned.md) (src/backend/catalog/pg_shdepend.c:1369)
  - [shdepReassignOwned](../s/shdepReassignOwned.md) (src/backend/catalog/pg_shdepend.c:1550)
  - [DropTableSpace](../D/DropTableSpace.md) (src/backend/commands/tablespace.c:448)
  - [typeDepNeeded](../t/typeDepNeeded.md) (src/backend/commands/opclasscmds.c:1685)

## Notes and Other Information
- Objects with OIDs >= FirstUnpinnedObjectId are never pinned, ensuring user-defined objects can always be dropped
- Large objects (LargeObjectRelationId) are explicitly excluded because their OIDs can be user-assigned
- The public namespace (PG_PUBLIC_NAMESPACE) is not pinned as a policy decision
- Databases are never pinned, allowing template0 and template1 to serve as mutual backups
- The function errs on the side of marking more objects as pinned rather than maintaining a precise minimal set
- This approach provides better performance than detailed dependency tracking while maintaining system integrity