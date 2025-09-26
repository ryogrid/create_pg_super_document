# ExecGrant_Attribute

## Location
[src/backend/catalog/aclchk.c:1680-1824](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L1680-L1824)

## Overview
Processes GRANT/REVOKE operations on individual column attributes, handling ACL modifications and dependency tracking for column-level privileges.

## Definition
```c
static void ExecGrant_Attribute(InternalGrant *istmt, Oid relOid, const char *relname,
                               AttrNumber attnum, Oid ownerId, AclMode col_privileges,
                               Relation attRelation, const Acl *old_rel_acl)
```

## Detailed Description
This static function handles the detailed processing of GRANT/REVOKE operations on individual column attributes. It is designed to be called from ExecGrant_Relation rather than directly from ExecuteGrantStmt. The function performs comprehensive ACL management including:

1. **ACL Retrieval**: Gets the existing column ACL or creates a default if none exists
2. **Permission Validation**: Merges table-level and column-level ACLs to determine what privileges the grantor can actually grant
3. **Privilege Restriction**: Uses restrict_and_check_grant to validate and restrict privileges according to SQL standards
4. **ACL Generation**: Creates new ACL by merging the grant/revoke operation with existing privileges
5. **Catalog Updates**: Updates pg_attribute with the new ACL and handles dependency tracking
6. **Optimization**: Avoids unnecessary updates when ACLs become empty (default state)

## Parameters
- `istmt`: InternalGrant structure containing details of the grant/revoke operation
- `relOid`: OID of the relation containing the attribute
- `relname`: Name of the relation (for error reporting)
- `attnum`: Attribute number of the specific column being processed
- `ownerId`: OID of the relation owner
- `col_privileges`: AclMode bitmask of privileges being granted/revoked on this column
- `attRelation`: Open relation handle for pg_attribute catalog
- `old_rel_acl`: Existing table-level ACL (used for permission validation)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclPCopy
  - [aclmembers](../a/aclmembers.md)
  - [aclconcat](../a/aclconcat.md)
  - [select_best_grantor](../s/select_best_grantor.md)
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [recordExtensionInitPriv](../r/recordExtensionInitPriv.md)
  - [updateAclDependencies](../u/updateAclDependencies.md)
- Called from (representative examples):
  - [ExecGrant_Relation](ExecGrant_Relation.md)

## Notes and Other Information
- This is a static function only used within aclchk.c as part of the relation-level grant processing
- Handles both explicit column privileges and default column ACL behavior
- Includes optimization to avoid unnecessary pg_attribute updates when ACLs are empty
- Merges table-level and column-level ACLs when determining grantor capabilities to ensure proper privilege inheritance
- Maintains shared dependency information to track role relationships with column privileges
- Records initial privileges for extension objects for proper privilege restoration during upgrades
- The function assumes that the default ACL state for columns is empty (no explicit entries)