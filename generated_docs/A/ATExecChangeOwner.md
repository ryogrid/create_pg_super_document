# ATExecChangeOwner

## Location
src/backend/commands/tablecmds.c: 14476 - 14716

## Overview
ATExecChangeOwner implements the ALTER TABLE OWNER command, changing the ownership of tables, views, indexes, sequences, and related objects while handling permission checks, ACL updates, and recursive ownership changes for dependent objects.

## Definition
```c
void
ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing, LOCKMODE lockmode)
```

## Detailed Description
This function handles ownership changes for various types of relations in PostgreSQL. It performs comprehensive validation of the target relation type, enforces permission requirements, and updates both the relation's owner and its access control lists. The function handles special cases for different relation kinds, including preventing direct ownership changes of indexes (suggesting users change the table owner instead) and sequences owned by tables.

When changing ownership, the function updates the pg_class catalog, adjusts ACLs for both the relation and its columns, updates dependency information, and recursively changes ownership of related objects like indexes, toast tables, sequences, and row types. The recursing parameter allows the function to skip permission checks when called recursively for dependent objects.

## Parameters
- `relationOid`: OID of the relation whose ownership is being changed
- `newOwnerId`: OID of the new owner
- `recursing`: True when recursively changing ownership of dependent objects (skips permission checks)
- `lockmode`: Lock mode to use when opening the relation

## Dependencies
- Functions called/Symbols referenced:
  - relation_open, relation_close, table_open, table_close
  - SearchSysCache1, ReleaseSysCache, SysCacheGetAttr
  - superuser, object_ownercheck, check_can_set_role, object_aclcheck
  - aclnewowner, change_owner_fix_column_acls, changeDependencyOnOwner
  - AlterTypeOwnerInternal, RelationGetIndexList
  - change_owner_recurse_to_sequences
  - sequenceIsOwned, errdetail_relkind_not_supported
  - heap_modify_tuple, CatalogTupleUpdate, heap_freetuple
  - InvokeObjectPostAlterHook
- Called from:
  - ATExecCmd (main ALTER TABLE command execution)
  - shdepReassignOwned_Owner (during REASSIGN OWNED operations)
  - AlterTypeOwner_oid (for composite types)
  - change_owner_recurse_to_sequences (for dependent sequences)
  - Recursively calls itself for indexes and toast tables

## Notes and Other Information
- Supports tables, views, materialized views, foreign tables, and partitioned tables
- Prevents direct ownership changes of indexes and partitioned indexes when not recursing
- Handles owned sequences by preventing ownership changes when they're linked to tables
- Updates column-level ACLs in addition to relation-level ACLs
- Recursively changes ownership of indexes, toast tables, and dependent sequences
- Also changes ownership of the relation's row type if it exists
- Fires post-alter hooks for proper event notification
- Uses appropriate error messages and hints for unsupported operations