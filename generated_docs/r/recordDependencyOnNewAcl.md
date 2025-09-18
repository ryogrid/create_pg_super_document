# recordDependencyOnNewAcl

## Location
[src/backend/catalog/aclchk.c:4382-4408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4382-L4408)

## Overview
Records dependency relationships between a newly created database object and all roles mentioned in its ACL to track privilege dependencies in the system catalog.

## Definition


## Detailed Description
This function establishes dependency tracking for ACL-related roles when a new database object is created with custom privileges. It extracts all role OIDs mentioned in the provided ACL using aclmembers(), then calls updateAclDependencies() to record these dependencies in the pg_shdepend system catalog.

The function serves as a specialized wrapper around updateAclDependencies() for the new object creation scenario. It handles the case where an object is being created with a non-default ACL obtained from get_user_default_acl() or similar functions. The dependency tracking ensures that roles referenced in ACLs cannot be dropped while objects depend on them for access control.

If the ACL parameter is NULL (indicating default system permissions), the function returns early without recording any dependencies, as default permissions don't create explicit role dependencies.

## Parameters / Member Variables
- : OID of the system catalog class (e.g., RelationRelationId for tables)
- : OID of the specific object being created
- : Sub-object identifier (0 for whole objects, positive for columns, etc.)
- : OID of the role that owns the object
- : The access control list containing role privileges (NULL for default permissions)

## Dependencies
- Functions called/Symbols referenced:
  - [aclmembers](../a/aclmembers.md) (extracts all role OIDs from an ACL)
  - [updateAclDependencies](../u/updateAclDependencies.md) (records/updates shared dependency information)
- Called from:
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md) (when creating tables with custom ACLs)
  - [NamespaceCreate](../N/NamespaceCreate.md) (when creating schemas with custom ACLs)
  - [ProcedureCreate](../P/ProcedureCreate.md) (when creating functions with custom ACLs)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md) (when creating types with custom ACLs)

## Notes and Other Information
- This function is specifically designed for new object creation scenarios
- Returns immediately if acl is NULL, avoiding unnecessary dependency tracking for default permissions
- Works in conjunction with get_user_default_acl() which may return non-NULL ACLs that need dependency tracking
- The function uses updateAclDependencies() with empty 'old' parameters (0, NULL) since this is a new object
- Part of PostgreSQL's shared dependency system that prevents dropping roles while objects depend on them
- Essential for maintaining referential integrity between roles and objects that reference them in ACLs
- Called after object creation but before transaction commit to ensure proper dependency tracking