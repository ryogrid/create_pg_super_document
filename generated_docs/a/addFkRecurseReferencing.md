# addFkRecurseReferencing

## Location
src/backend/commands/tablecmds.c: 10428 - 10603

## Overview
Recursively handles the referencing side of foreign key creation by creating check triggers for regular tables and recursing through partitions for partitioned tables, with support for constraint reuse and validation scheduling.

## Definition
```c
static void addFkRecurseReferencing(List **wqueue, Constraint *fkconstraint, Relation rel,
                                   Relation pkrel, Oid indexOid, Oid parentConstr,
                                   int numfks, int16 *pkattnum, int16 *fkattnum,
                                   Oid *pfeqoperators, Oid *ppeqoperators, Oid *ffeqoperators,
                                   int numfkdelsetcols, int16 *fkdelsetcols,
                                   bool old_check_ok, LOCKMODE lockmode,
                                   Oid parentInsTrigger, Oid parentUpdTrigger)
```

## Detailed Description
This function manages the referencing (foreign key) side of constraint creation with sophisticated handling for partitioned tables. It creates check triggers that enforce referential integrity and optionally schedules constraint validation for Phase 3 processing during ALTER TABLE operations.

The function operates differently based on the relation type:

For regular relations:
- Creates check triggers via createForeignKeyCheckTriggers
- Schedules constraint validation if part of an ALTER TABLE operation and validation is required

For partitioned tables:
- Recursively processes each partition with proper attribute mapping
- Attempts to reuse existing compatible foreign key constraints via tryAttachPartitionForeignKey
- Creates new constraints when reuse is not possible
- Manages trigger catalog relation efficiently during recursion

Key features include constraint reuse optimization (avoiding duplicate constraints when possible), proper attribute mapping for partitions, efficient catalog management during recursion, and integration with ALTER TABLE work queue for validation scheduling.

## Parameters / Member Variables
- `wqueue`: ALTER TABLE work queue for scheduling validation (NULL when not part of ALTER TABLE)
- `fkconstraint`: The constraint definition being processed
- `rel`: The referencing relation (foreign key table, may be a partition)
- `pkrel`: The root referenced relation (primary key table)
- `indexOid`: OID of the index implementing this constraint on pkrel
- `parentConstr`: OID of the parent constraint (always valid for this function)
- `numfks`: Number of columns in the foreign key
- `pkattnum`: Array of attribute numbers for referenced columns
- `fkattnum`: Array of attribute numbers for referencing columns
- `pfeqoperators`: Array of equality operators between PK and FK columns
- `ppeqoperators`: Array of equality operators for PK columns
- `ffeqoperators`: Array of equality operators for FK columns
- `numfkdelsetcols`: Number of columns in ON DELETE SET NULL/DEFAULT clause
- `fkdelsetcols`: Array of attribute numbers for SET action columns
- `old_check_ok`: Whether existing validation can be trusted (skip revalidation)
- `lockmode`: Lock mode to acquire on partitions during recursion
- `parentInsTrigger`: OID of parent INSERT trigger (for partition recursion)
- `parentUpdTrigger`: OID of parent UPDATE trigger (for partition recursion)

## Dependencies
- Functions called/Symbols referenced:
  - CheckRelationLockedByMe
  - createForeignKeyCheckTriggers
  - ATGetQueueEntry
  - get_constraint_name
  - RelationGetPartitionDesc
  - CheckAlterTableIsSafe
  - build_attrmap_by_name
  - RelationGetFKeyList
  - copyObject
  - tryAttachPartitionForeignKey
  - addFkConstraint
  - addFkRecurseReferencing (recursive call)
  - table_open/table_close
- Called from (representative examples):
  - ATAddForeignKeyConstraint
  - addFkRecurseReferencing (recursive)
  - CloneFkReferencing

## Notes and Other Information
- This is a static function within tablecmds.c, part of the foreign key constraint creation infrastructure
- The function is recursive and calls itself when processing partitioned tables
- Foreign tables are explicitly rejected with an error message
- Constraint reuse optimization can significantly improve performance when adding foreign keys to partitioned tables with existing compatible constraints
- The work queue integration allows proper validation scheduling during ALTER TABLE operations
- Proper lock management ensures consistency during concurrent operations
- The function efficiently manages the trigger catalog relation during partition processing to avoid excessive open/close operations
- Memory management includes proper cleanup of attribute maps and copied objects
- Phase 3 validation is only scheduled for regular relations that require it (not partitioned tables themselves)