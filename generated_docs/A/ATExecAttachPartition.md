# ATExecAttachPartition

## Location
[src/backend/commands/tablecmds.c:18487-18802](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L18487-L18802)

## Overview
ATExecAttachPartition implements the ALTER TABLE ATTACH PARTITION command, performing comprehensive validation and setup to attach a new table as a partition to a partitioned table.

## Definition
```c
static ObjectAddress ATExecAttachPartition(List **wqueue, Relation rel, PartitionCmd *cmd,
                                           AlterTableUtilityContext *context)
```

## Detailed Description
This function handles the complex process of attaching a table as a partition to a partitioned table. The operation involves extensive validation, constraint setup, and metadata updates:

**Validation Phase:**
- Checks permissions on both parent and child tables
- Validates that the table is not already a partition
- Prevents circular inheritance relationships
- Ensures compatible persistence levels (temporary/permanent)
- Validates column compatibility between parent and child
- Checks for incompatible features (identity columns, certain triggers)

**Setup Phase:**
- Establishes inheritance relationship via CreateInheritance
- Updates partition boundary information in pg_class
- Ensures matching indexes exist on the partition
- Clones row triggers and foreign key constraints
- Generates and validates partition constraints

**Constraint Management:**
- Creates partition boundary constraints from the FOR VALUES specification
- Combines with parent partition quals if the parent is itself a partition
- Queues constraint validation work for the new partition
- Updates default partition constraints if a default partition exists

The function integrates with PostgreSQL's three-phase ALTER TABLE model by queuing validation work for Phase 3 execution.

## Parameters / Member Variables
- `wqueue`: Work queue for storing validation tasks to be executed in Phase 3
- `rel`: The parent partitioned table relation
- `cmd`: PartitionCmd containing the partition boundary specification and table name
- `context`: ALTER TABLE context containing query string and other metadata

## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md), get_default_oid_from_partdesc, LockRelationOid
  - table_openrv, ATSimplePermissions, find_all_inheritors
  - [check_new_partition_bound](../c/check_new_partition_bound.md), CreateInheritance, StorePartitionBound
  - [AttachPartitionEnsureIndexes](AttachPartitionEnsureIndexes.md), CloneRowTriggersToPartition, CloneForeignKeyConstraints
  - [get_qual_from_partbound](../g/get_qual_from_partbound.md), RelationGetPartitionQual, QueuePartitionConstraintValidation
  - [get_proposed_default_constraint](../g/get_proposed_default_constraint.md), map_partition_varattnos
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type

## Notes and Other Information
- Static function used internally within ALTER TABLE processing
- Uses AccessExclusiveLock throughout to prevent concurrent modifications
- Handles both regular and partitioned tables as attachments
- Updates default partition constraints when attaching non-default partitions
- Invalidates relcache for descendent partitions when attaching partitioned tables
- Prevents attachment of tables with identity columns or incompatible triggers
- Maintains locks until transaction commit for consistency
- Returns ObjectAddress pointing to the newly attached partition
- Part of PostgreSQL's comprehensive partitioning infrastructure