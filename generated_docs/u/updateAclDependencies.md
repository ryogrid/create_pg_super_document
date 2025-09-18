# updateAclDependencies

## Location
src/backend/catalog/pg_shdepend.c: 491 - 511

## Overview
Updates shared dependency records for an object's ACL during GRANT/REVOKE operations, tracking which roles have privileges on database objects.

## Definition
```c
void updateAclDependencies(Oid classId, Oid objectId, int32 objsubId,
                          Oid ownerId,
                          int noldmembers, Oid *oldmembers,
                          int nnewmembers, Oid *newmembers)
```

## Detailed Description
This function serves as a public interface for updating ACL-related shared dependencies when GRANT or REVOKE operations occur. It calculates the differences between old and new ACL member lists and updates the pg_shdepend catalog accordingly. The function delegates the actual work to updateAclDependenciesWorker, passing SHARED_DEPENDENCY_ACL as the dependency type.

The function is designed to handle the complexity of ACL updates efficiently by comparing old and new privilege lists rather than blindly inserting or deleting dependencies. This approach avoids duplicate dependencies during GRANT operations and prevents incorrect deletions during REVOKE when users may still have other privileges.

## Parameters / Member Variables
- `classId`: OID of the system catalog class containing the object (e.g., RelationRelationId)
- `objectId`: OID of the specific object whose ACL is being updated
- `objsubId`: Sub-object identifier (e.g., column number for table columns, 0 for whole objects)
- `ownerId`: OID of the role that owns the object
- `noldmembers`: Number of roles in the old ACL array
- `oldmembers`: Array of role OIDs that appeared in the previous ACL (must be sorted and de-duped)
- `nnewmembers`: Number of roles in the new ACL array  
- `newmembers`: Array of role OIDs that appear in the updated ACL (must be sorted and de-duped)

## Dependencies
- Functions called/Symbols referenced:
  - [updateAclDependenciesWorker](updateAclDependenciesWorker.md)
  - SHARED_DEPENDENCY_ACL
- Called from (representative examples):
  - [SetDefaultACL](../S/SetDefaultACL.md)
  - [ExecGrant_Attribute](../E/ExecGrant_Attribute.md)
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md)
  - [ExecGrant_common](../E/ExecGrant_common.md)
  - [ExecGrant_Largeobject](../E/ExecGrant_Largeobject.md)
  - [AddRoleMems](../A/AddRoleMems.md)
  - [recordDependencyOnNewAcl](../r/recordDependencyOnNewAcl.md)

## Notes and Other Information
- Both input arrays must be sorted and de-duplicated before calling (typically via aclmembers())
- Input arrays are freed before the function returns
- Handles complex scenarios where REVOKE may actually add dependencies due to default ACL instantiation
- Part of PostgreSQL's shared dependency tracking system for cross-database privilege relationships
- Owner dependencies are typically ignored as they are handled separately