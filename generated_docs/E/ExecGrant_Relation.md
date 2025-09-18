# ExecGrant_Relation

## Location
src/backend/catalog/aclchk.c: 1825 - 2155

## Overview
Processes GRANT/REVOKE operations on relations (tables, sequences), handling both relation-level and column-level privileges with comprehensive validation and ACL management.

## Definition
```c
static void ExecGrant_Relation(InternalGrant *istmt)
```

## Detailed Description
This static function is the main workhorse for processing GRANT/REVOKE operations on relations, including both regular tables and sequences. It handles complex privilege management scenarios including:

1. **Object Validation**: Validates that the target objects are appropriate for GRANT operations (not indexes or composite types)
2. **Privilege Type Handling**: Adjusts privilege types based on object kind (sequence vs. table) and validates privilege compatibility
3. **Column Privilege Processing**: Handles both explicit column privileges and implicit column privilege revocation during relation-level REVOKE operations
4. **ACL Management**: Creates, updates, and manages Access Control Lists for both relation-level and column-level privileges
5. **Dependency Tracking**: Updates shared dependency information to track role relationships with privileges
6. **Catalog Updates**: Updates both pg_class (for relation ACLs) and pg_attribute (for column ACLs) system catalogs

The function processes each relation in the istmt->objects list, handling relation-level privileges first, then processing any column-specific privileges. It includes extensive validation and error handling for various edge cases and object type combinations.

## Parameters
- `istmt`: InternalGrant structure containing complete details of the grant/revoke operation including:
  - Object list, privilege specifications, grantees, grant options, and operation type

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [SearchSysCacheLocked1](../S/SearchSysCacheLocked1.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclPCopy
  - [aclmembers](../a/aclmembers.md)
  - aclcopy
  - select_best_grantor
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [expand_all_col_privileges](../e/expand_all_col_privileges.md)
  - [expand_col_privileges](../e/expand_col_privileges.md)
  - [ExecGrant_Attribute](ExecGrant_Attribute.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - [recordExtensionInitPriv](../r/recordExtensionInitPriv.md)
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - CommandCounterIncrement
- Called from (representative examples):
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md)

## Notes and Other Information
- This is a static function only used within aclchk.c as part of the grant/revoke processing pipeline
- Handles both tables and sequences with different privilege sets (sequences support USAGE, SELECT, UPDATE)
- Implements SQL standard requirement that REVOKE on relation-level privileges also revokes corresponding column-level privileges
- Includes backward compatibility warnings for invalid privilege types on sequences when using generic TABLE syntax
- Manages complex column privilege arrays indexed from FirstLowInvalidHeapAttributeNumber to handle system columns
- Performs optimization to avoid unnecessary catalog updates when privileges don't actually change
- Uses tuple locking mechanisms to ensure consistency during concurrent operations
- Validates that column privileges are appropriate for the object type (sequences only support SELECT on columns)
- Records initial privileges for extension objects to support proper privilege restoration during upgrades