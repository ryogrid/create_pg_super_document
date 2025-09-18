# IsPinnedObject

## Location
src/backend/catalog/catalog.c: 343 - 420

## Overview
IsPinnedObject determines whether a database object is "pinned" and therefore cannot be dropped because the system requires it to function properly.

## Definition


## Detailed Description
This function identifies system objects that are essential to PostgreSQL's operation and cannot be dropped by users. Instead of maintaining explicit dependency records in pg_depend (which proved to be expensive overhead), it uses an efficient OID range test combined with specific class-based exceptions. Objects with OIDs below FirstUnpinnedObjectId are generally considered pinned, representing initdb-created system objects. The function includes several policy-based exceptions, such as large objects (which can have user-assigned OIDs), the public namespace, and databases (including templates), which are deliberately not pinned to allow certain administrative operations.

## Parameters / Member Variables
- : The OID of the system catalog (pg_class entry) that contains the object
- : The OID of the specific object to check

## Dependencies
- Functions called/Symbols referenced:
  - FirstUnpinnedObjectId (constant defining the boundary between system and user objects)
- Called from (representative examples):
  - findDependentObjects (src/backend/catalog/dependency.c:494)
  - isObjectPinned (src/backend/catalog/pg_depend.c:712)
  - recordSharedDependencyOn (src/backend/catalog/pg_shdepend.c:147)
  - checkSharedDependencies (src/backend/catalog/pg_shdepend.c:696)
  - shdepDropOwned (src/backend/catalog/pg_shdepend.c:1369)
  - shdepReassignOwned (src/backend/catalog/pg_shdepend.c:1550)
  - DropTableSpace (src/backend/commands/tablespace.c:448)
  - typeDepNeeded (src/backend/commands/opclasscmds.c:1685)

## Notes and Other Information
- Objects with OIDs >= FirstUnpinnedObjectId are never pinned, ensuring user-defined objects can always be dropped
- Large objects (LargeObjectRelationId) are explicitly excluded because their OIDs can be user-assigned
- The public namespace (PG_PUBLIC_NAMESPACE) is not pinned as a policy decision
- Databases are never pinned, allowing template0 and template1 to serve as mutual backups
- The function errs on the side of marking more objects as pinned rather than maintaining a precise minimal set
- This approach provides better performance than detailed dependency tracking while maintaining system integrity