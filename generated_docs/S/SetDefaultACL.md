# SetDefaultACL

## Location
src/backend/catalog/aclchk.c: 1203 - 1465

## Overview
Creates or updates a pg_default_acl catalog entry to store default access control privileges for future objects of a specific type, role, and namespace combination.

## Definition


## Detailed Description
This function implements the core logic for managing default ACL entries in the pg_default_acl system catalog. It handles both database-wide (global) and schema-specific default privileges for various object types (tables, sequences, functions, types, schemas). The function determines the appropriate default ACL baseline - for global entries, it uses hard-wired defaults via acldefault(), while schema-specific entries start with an empty ACL. It searches for existing entries using a three-key cache lookup (role, namespace, object type), then merges the requested privilege changes with the existing ACL using merge_acl_with_grant(). If the result equals the default ACL, the entry is deleted; otherwise, it's inserted or updated. The function also manages dependency relationships and shared dependency ACL information to maintain referential integrity.

## Parameters / Member Variables
- : Pointer to InternalDefaultACL structure containing all the privilege specification details including role ID, namespace ID, object type, privileges, grantees, and grant options

## Dependencies
- Functions called/Symbols referenced:
  - acldefault
  - make_empty_acl
  - SearchSysCache3
  - SysCacheGetAttr
  - DatumGetAclPCopy
  - aclmembers
  - aclcopy
  - merge_acl_with_grant
  - aclitemsort
  - aclequal
  - performDeletion
  - GetNewOidWithIndex
  - heap_form_tuple
  - CatalogTupleInsert
  - heap_modify_tuple
  - CatalogTupleUpdate
  - recordDependencyOnOwner
  - recordDependencyOn
  - updateAclDependencies
  - InvokeObjectPostCreateHook
  - InvokeObjectPostAlterHook
  - CommandCounterIncrement
- Called from (representative examples):
  - SetDefaultACLsInSchemas
  - RemoveRoleFromObjectACL

## Notes and Other Information
- The function is static and serves as the lowest-level implementation for default ACL management
- Global entries (nspid = InvalidOid) replace hard-wired defaults, while schema-specific entries are additive
- Object types are converted from OBJECT_* constants to DEFACLOBJ_* constants for catalog storage
- The 'all_privs' flag automatically expands to the appropriate ACL_ALL_RIGHTS_* constant for the object type
- Schema-specific default privileges for schemas themselves are explicitly forbidden and will generate an error
- ACL comparison requires sorting both ACLs to ensure accurate equality checks
- The function uses RowExclusiveLock on pg_default_acl to prevent concurrent modifications
- Dependency management includes both ownership dependencies (on the role) and usage dependencies (on the namespace)
- CommandCounterIncrement() prevents issues when processing duplicate objects in the same command
- Post-creation/alteration hooks are invoked for proper event notification