# deleteSharedDependencyRecordsFor

## Location
[src/backend/catalog/pg_shdepend.c:1047-1068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1047-L1068)

## Overview
Removes all shared dependency entries for an object being dropped or modified, handling both whole objects and individual sub-objects with appropriate cleanup scope.

## Definition
```c
void deleteSharedDependencyRecordsFor(Oid classId, Oid objectId, int32 objectSubId)
```

## Detailed Description
This function provides a clean interface for removing shared dependency records associated with an object that is being dropped or modified. It determines whether to delete dependencies for just a specific sub-object or for an entire object hierarchy based on the objectSubId parameter. When objectSubId is zero, indicating a whole object deletion, the function removes dependencies for all sub-objects as well, ensuring complete cleanup. The function works with both shared objects (like roles, tablespaces) and local database objects, with the classId parameter determining the object type.

## Parameters / Member Variables
- `classId`: OID of the system catalog that contains the object (determines object type)
- `objectId`: OID of the specific object whose dependencies should be removed
- `objectSubId`: Sub-object identifier; when 0, indicates whole object deletion including all sub-objects

## Dependencies
- Functions called/Symbols referenced:
  - [shdepDropDependency](../s/shdepDropDependency.md) (performs the actual dependency removal)
  - SHARED_DEPENDENCY_INVALID (dependency type constant indicating removal)
- Called from (representative examples):
  - [deleteOneObject](deleteOneObject.md) (general object deletion in dependency.c:1322)
  - [DropRole](../D/DropRole.md) (role deletion in user.c:1223, 1243)
  - [DropTableSpace](../D/DropTableSpace.md) (tablespace deletion in tablespace.c:481)
  - [DropSubscription](../D/DropSubscription.md) (subscription deletion in subscriptioncmds.c:1730)
  - [DelRoleMems](../D/DelRoleMems.md) (role membership removal in user.c:2053)
  - [makeOperatorDependencies](../m/makeOperatorDependencies.md) (operator dependency management in pg_operator.c:871)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md) (type dependency management in pg_type.c:596)

## Notes and Other Information
- Simple wrapper around shdepDropDependency that provides a convenient interface for dependency cleanup
- Automatically handles sub-object cleanup when objectSubId is 0 by passing true for the dropSubObjects parameter
- Uses RowExclusiveLock on SharedDependRelationId to ensure consistent dependency modifications
- Critical component of PostgreSQL's cascading deletion and object modification system
- Works with both shared objects (accessible across databases) and local objects (database-specific)
- Used extensively throughout the system whenever objects with potential shared dependencies are modified or removed
- Helps maintain referential integrity by ensuring orphaned dependency records are not left behind

## Simplified Source

```c
void deleteSharedDependencyRecordsFor(Oid classId, Oid objectId, int32 objectSubId) {
    Relation sdepRel;

    // Open shared dependency catalog with exclusive lock
    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);

    // Remove dependency records
    shdepDropDependency(sdepRel, classId, objectId, objectSubId,
                       (objectSubId == 0),  // Drop sub-objects if whole object
                       InvalidOid, InvalidOid,
                       SHARED_DEPENDENCY_INVALID);

    // Close the catalog
    table_close(sdepRel, RowExclusiveLock);
}
```