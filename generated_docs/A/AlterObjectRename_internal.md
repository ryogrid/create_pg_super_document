# AlterObjectRename_internal

## Location
src/backend/commands/alter.c: 165 - 356

## Overview
A generic internal function that renames database objects by updating their name column in the appropriate catalog relation, handling permission checks and duplicate name detection.

## Definition
```c
static void AlterObjectRename_internal(Relation rel, Oid objectId, const char *new_name)
```

## Detailed Description
This function provides a generic mechanism for renaming various types of database objects that can be renamed by simply changing their name column in a single catalog table. It performs comprehensive permission checks, validates ownership, checks for naming conflicts, and updates the catalog entry. The function handles objects with and without namespaces, and includes special logic for specific object types like subscriptions, procedures, collations, operator classes, and operator families. It uses PostgreSQL's catalog cache system for efficient lookups and maintains referential integrity through dependency tracking.

## Parameters / Member Variables
- `rel`: Catalog relation containing the object (must be opened with RowExclusiveLock by caller)
- `objectId`: OID of the object to be renamed
- `new_name`: C string representation of the new name for the object

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid (get relation OID)
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md)/name (cache ID functions)
  - [get_object_attnum_name](../g/get_object_attnum_name.md)/namespace/owner (attribute number functions)
  - [SearchSysCache1](../S/SearchSysCache1.md) (cache lookup)
  - [heap_getattr](../h/heap_getattr.md) (extract attributes from tuples)
  - superuser (check superuser privileges)
  - has_privs_of_role (role privilege checking)
  - [object_aclcheck](../o/object_aclcheck.md) (access control checking)
  - [aclcheck_error](../a/aclcheck_error.md) (ACL error reporting)
  - Various object-specific existence check functions (IsThereFunctionInNamespace, etc.)
  - [report_name_conflict](../r/report_name_conflict.md)/report_namespace_conflict (conflict reporting)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (tuple modification)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog update)
  - InvokeObjectPostAlterHook (post-alter hook)
  - [LogicalRepWorkersWakeupAtCommit](../L/LogicalRepWorkersWakeupAtCommit.md) (subscription-specific)

- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md) (src/backend/commands/alter.c:434)

## Notes and Other Information
- This is a static function, only accessible within src/backend/commands/alter.c
- Designed for simple rename operations where only the name column needs to be updated
- Not suitable for tables or complex objects requiring additional structural changes
- Includes comprehensive permission checking: superuser bypass, ownership verification, and namespace CREATE privileges
- Handles special cases for subscriptions including password_required validation and replication worker notification
- Uses object-specific duplicate name checking functions for procedures, collations, operator classes, and families
- Employs PostgreSQL's heap tuple modification and catalog update mechanisms
- Memory management includes proper cleanup of allocated arrays and tuples
- Supports both namespace-aware and global objects through conditional namespace handling