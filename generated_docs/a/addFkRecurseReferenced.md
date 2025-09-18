# addFkRecurseReferenced

## Location
[src/backend/commands/tablecmds.c:10296-10427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L10296-L10427)

## Overview
Recursively handles the referenced side of foreign key creation by creating action triggers for regular tables and recursing through partitions for partitioned tables, establishing constraint entries at each level.

## Definition
```c
static void addFkRecurseReferenced(Constraint *fkconstraint, Relation rel,
                                  Relation pkrel, Oid indexOid, Oid parentConstr,
                                  int numfks,
                                  int16 *pkattnum, int16 *fkattnum, Oid *pfeqoperators,
                                  Oid *ppeqoperators, Oid *ffeqoperators,
                                  int numfkdelsetcols, int16 *fkdelsetcols,
                                  bool old_check_ok,
                                  Oid parentDelTrigger, Oid parentUpdTrigger)
```

## Detailed Description
This function handles the complex task of setting up foreign key constraints on the referenced (primary key) side, with special handling for partitioned tables. It creates the necessary action triggers that enforce referential integrity and recursively processes all partitions when dealing with partitioned tables.

The function operates in two main phases:
1. Creates action triggers for the current referenced relation using createForeignKeyActionTriggers
2. If the referenced table is partitioned, recursively processes each partition by:
   - Opening each partition with appropriate locks
   - Mapping attribute numbers to match partition column layouts
   - Finding the corresponding partition index
   - Creating constraint entries for each partition
   - Recursively calling itself for nested partitions

Key responsibilities include attribute mapping for partitions (since column orders may differ), proper lock management throughout the recursion, index resolution for each partition level, and maintaining constraint hierarchy through parent-child relationships.

## Parameters / Member Variables
- `fkconstraint`: The constraint definition being processed
- `rel`: The root referencing relation (foreign key table)
- `pkrel`: The referenced relation (primary key table, may be a partition)
- `indexOid`: OID of the index implementing this constraint on pkrel
- `parentConstr`: OID of parent constraint (InvalidOid for top-level)
- `numfks`: Number of columns in the foreign key
- `pkattnum`: Array of attribute numbers for referenced columns
- `fkattnum`: Array of attribute numbers for referencing columns
- `pfeqoperators`: Array of equality operators between PK and FK columns
- `ppeqoperators`: Array of equality operators for PK columns
- `ffeqoperators`: Array of equality operators for FK columns
- `numfkdelsetcols`: Number of columns in ON DELETE SET NULL/DEFAULT clause
- `fkdelsetcols`: Array of attribute numbers for SET action columns
- `old_check_ok`: Whether existing validation can be trusted (skip revalidation)
- `parentDelTrigger`: OID of parent DELETE trigger (for partition recursion)
- `parentUpdTrigger`: OID of parent UPDATE trigger (for partition recursion)

## Dependencies
- Functions called/Symbols referenced:
  - [CheckRelationLockedByMe](../C/CheckRelationLockedByMe.md)
  - [createForeignKeyActionTriggers](../c/createForeignKeyActionTriggers.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - table_open
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [index_get_partition](../i/index_get_partition.md)
  - [addFkConstraint](addFkConstraint.md)
  - [addFkRecurseReferenced](addFkRecurseReferenced.md) (recursive call)
  - table_close
  - [free_attrmap](../f/free_attrmap.md)
  - RelationGetDescr
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)
  - [addFkRecurseReferenced](addFkRecurseReferenced.md) (recursive)
  - [CloneFkReferenced](../C/CloneFkReferenced.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)

## Notes and Other Information
- This is a static function within tablecmds.c, part of the foreign key constraint creation infrastructure
- The function is recursive and calls itself when processing partitioned tables
- Proper lock management is critical - the function verifies locks are held and maintains them throughout recursion
- Attribute mapping is essential for partitions since column orders may differ from the parent table
- The function handles both regular tables (creates triggers) and partitioned tables (recurses through partitions)
- Memory management includes proper cleanup of attribute maps and maintaining lock consistency
- The function is designed to work with the partitioning system introduced in PostgreSQL 10+
- Triggers created at each level enforce referential integrity for that specific relation/partition