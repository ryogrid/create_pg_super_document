# updateAclDependenciesWorker

## Location
[src/backend/catalog/pg_shdepend.c:525-603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L525-L603)

## Overview
Core worker function that performs the actual shared dependency updates for ACL and initial ACL changes, handling the addition and removal of role-based dependencies.

## Definition
```c
static void updateAclDependenciesWorker(Oid classId, Oid objectId, int32 objsubId,
                                       Oid ownerId, SharedDependencyType deptype,
                                       int noldmembers, Oid *oldmembers,
                                       int nnewmembers, Oid *newmembers)
```

## Detailed Description
This static function implements the core logic for updating shared dependencies when ACLs change. It first uses getOidListDiff() to identify roles that need to be added or removed from the dependency tracking, then performs the actual catalog updates. The function handles two types of dependencies: SHARED_DEPENDENCY_ACL for regular privileges and SHARED_DEPENDENCY_INITACL for initial privileges.

The function optimizes updates by only processing roles that have actually changed, skipping common elements between old and new ACLs. It applies specific business rules like excluding owner roles from ACL dependency tracking (since they have separate OWNER dependencies) and skipping pinned system roles that don't require dependency entries.

## Parameters / Member Variables
- `classId`: OID of the system catalog class containing the object
- `objectId`: OID of the specific object whose dependencies are being updated
- `objsubId`: Sub-object identifier (0 for whole objects, column number for attributes)
- `ownerId`: OID of the object's owner (used to skip owner in ACL dependencies)
- `deptype`: Type of shared dependency (SHARED_DEPENDENCY_ACL or SHARED_DEPENDENCY_INITACL)
- `noldmembers`: Number of roles in the previous ACL array
- `oldmembers`: Array of role OIDs from previous ACL (freed by this function)
- `nnewmembers`: Number of roles in the updated ACL array
- `newmembers`: Array of role OIDs in updated ACL (freed by this function)

## Dependencies
- Functions called/Symbols referenced:
  - [getOidListDiff](../g/getOidListDiff.md)
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - [shdepAddDependency](../s/shdepAddDependency.md)
  - [shdepDropDependency](../s/shdepDropDependency.md)
  - [IsPinnedObject](../I/IsPinnedObject.md)
  - [pfree](../p/pfree.md)
  - SHARED_DEPENDENCY_ACL
  - SharedDependencyType
- Called from (representative examples):
  - [updateAclDependencies](updateAclDependencies.md)
  - [updateInitAclDependencies](updateInitAclDependencies.md)

## Notes and Other Information
- Static function only accessible within pg_shdepend.c
- Handles both addition of new dependencies and removal of obsolete ones in a single operation
- Skips owner role for ACL dependencies but includes owner for INITACL dependencies
- Automatically excludes pinned system roles from dependency tracking
- Uses RowExclusiveLock on SharedDependRelationId for atomic updates
- Frees input arrays before returning to prevent memory leaks
- Core implementation shared by both public ACL dependency update functions