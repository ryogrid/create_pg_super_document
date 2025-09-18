# ExecGrant_common

## Location
src/backend/catalog/aclchk.c: 2156 - 2291

## Overview
ExecGrant_common is the core implementation function for processing GRANT and REVOKE statements on database objects, handling the common ACL modification logic across different object types.

## Definition


## Detailed Description
ExecGrant_common performs the core work of granting or revoking privileges on database objects. It iterates through each object specified in the GRANT/REVOKE statement, retrieves the current ACL, applies the privilege changes using merge_acl_with_grant, and updates the system catalogs. The function handles privilege validation, grantor selection, and maintains dependency tracking for proper cleanup when roles are dropped.

The function is designed to work with any catalog table that stores ACLs by accepting a classid parameter and optional object-specific validation callback. It ensures atomicity by using appropriate locking and handles duplicate objects gracefully.

## Parameters / Member Variables
- : Internal representation of the GRANT/REVOKE statement containing grantees, privileges, and options
- : OID of the system catalog class (e.g., RelationRelationId for tables)
- : Default privileges to grant when ALL PRIVILEGES is specified  
- : Optional callback function for object-type-specific validation

## Dependencies
- Functions called/Symbols referenced:
  - get_object_catcache_oid
  - table_open, table_close
  - SearchSysCacheLocked1, ReleaseSysCache
  - SysCacheGetAttr, SysCacheGetAttrNotNull
  - acldefault, aclmembers
  - select_best_grantor
  - restrict_and_check_grant
  - merge_acl_with_grant
  - heap_modify_tuple, CatalogTupleUpdate
  - updateAclDependencies
  - recordExtensionInitPriv
  - CommandCounterIncrement
- Called from:
  - ExecGrantStmt_oids (for various object types like tables, functions, databases, etc.)

## Notes and Other Information
- Uses row-exclusive locking on the target catalog to prevent concurrent modifications
- Handles both explicit privilege lists and ALL PRIVILEGES grants
- Maintains shared dependency records to track which roles have privileges on objects
- Records extension privileges for proper pg_dump/restore handling
- Increments command counter after each object to handle duplicate processing
- The object_check callback allows type-specific validation (e.g., checking language trust for procedural languages)