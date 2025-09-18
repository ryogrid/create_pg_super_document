# updateInitAclDependencies

## Location
src/backend/catalog/pg_shdepend.c: 512 - 524

## Overview
Updates shared dependency records for initial privileges (pg_init_privs) entries, tracking role dependencies for default privileges on database objects.

## Definition
```c
void updateInitAclDependencies(Oid classId, Oid objectId, int32 objsubId,
                              int noldmembers, Oid *oldmembers,
                              int nnewmembers, Oid *newmembers)
```

## Detailed Description
This function manages shared dependencies for initial privilege records stored in pg_init_privs, which tracks the original privileges that were granted when an object was created (especially important for extension objects). Unlike regular ACL dependencies, initial privileges are recorded uniformly for both owners and non-owners, so no owner ID is needed.

The function delegates to updateAclDependenciesWorker with SHARED_DEPENDENCY_INITACL as the dependency type and InvalidOid as the owner ID (since owner distinctions don't apply for initial privileges). This ensures that initial privilege grants are properly tracked for dependency management without the complexity of owner-specific handling.

## Parameters / Member Variables
- `classId`: OID of the system catalog class containing the object (e.g., RelationRelationId)
- `objectId`: OID of the specific object whose initial ACL dependencies are being updated
- `objsubId`: Sub-object identifier (e.g., column number for table columns, 0 for whole objects)
- `noldmembers`: Number of roles in the previous initial privileges array
- `oldmembers`: Array of role OIDs from the previous initial privileges (must be sorted and de-duped)
- `nnewmembers`: Number of roles in the updated initial privileges array
- `newmembers`: Array of role OIDs in the updated initial privileges (must be sorted and de-duped)

## Dependencies
- Functions called/Symbols referenced:
  - [updateAclDependenciesWorker](updateAclDependenciesWorker.md)
  - SHARED_DEPENDENCY_INITACL
- Called from (representative examples):
  - [recordExtensionInitPrivWorker](../r/recordExtensionInitPrivWorker.md)
  - [ReplaceRoleInInitPriv](../R/ReplaceRoleInInitPriv.md)
  - [RemoveRoleFromInitPriv](../R/RemoveRoleFromInitPriv.md)

## Notes and Other Information
- Similar to updateAclDependencies but specifically for pg_init_privs entries
- Does not require an owner ID parameter since initial privilege recording treats owners and non-owners uniformly
- Passes InvalidOid as owner ID to indicate owner-specific logic should be bypassed
- Essential for proper dependency tracking of extension-granted privileges
- Input arrays must be sorted and de-duplicated before calling
- Part of PostgreSQL's initial privilege preservation system for extensions