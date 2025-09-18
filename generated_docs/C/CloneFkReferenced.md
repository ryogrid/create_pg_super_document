# CloneFkReferenced

## Location
src/backend/commands/tablecmds.c: 10633 - 10829

## Overview
CloneFkReferenced handles cloning foreign key constraints where the parent relation appears on the referenced side, used when creating or attaching partitions.

## Definition


## Detailed Description
This function is a subroutine for CloneForeignKeyConstraints that finds all foreign key constraints where the parent relation is on the referenced (target) side and clones those constraints to the given partition. The function is called when a partition is being created or attached to ensure that foreign key relationships are properly maintained across the partition hierarchy.

The function operates in two phases to avoid duplicates:
1. First, it scans pg_constraint to build a list of all foreign key constraints that reference the parent relation
2. Then it processes this list, cloning only those constraints whose parent constraints are not also being cloned

For each constraint to be cloned, the function:
- Maps column attributes from parent to partition using build_attrmap_by_name
- Creates a new Constraint node with the same properties as the original
- Finds the corresponding partition index using index_get_partition
- Retrieves action triggers from the original constraint
- Calls addFkConstraint to create the new constraint on the partition
- Recursively processes any sub-partitions via addFkRecurseReferenced

## Parameters / Member Variables
- : The parent relation that is being referenced by foreign keys
- : The partition relation where the cloned constraints will be created

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close: Access system catalogs
  - ScanKeyInit/systable_beginscan/systable_getnext: Scan pg_constraint catalog
  - build_attrmap_by_name: Map attributes between parent and partition
  - DeconstructFkConstraintRow: Extract FK constraint details from catalog tuple
  - GetForeignKeyActionTriggers: Retrieve action trigger OIDs for constraint
  - addFkConstraint: Create the actual foreign key constraint on partition
  - addFkRecurseReferenced: Recursively handle sub-partitions
  - index_get_partition: Find partition's corresponding index
  - makeNode: Create new Constraint node
  - list_member_oid: Check for duplicate constraints

- Called from:
  - CloneForeignKeyConstraints: Main entry point for cloning FK constraints during partition operations

## Notes and Other Information
- This function specifically handles the "referenced side" of foreign keys, meaning constraints where other tables reference this relation
- Uses a two-phase approach to prevent constraint duplication when both parent and child constraints would be cloned
- Requires appropriate locking (ShareRowExclusiveLock) on the referencing relation to safely create triggers
- The function handles both simple partitions and nested partitioning through recursive calls
- Part of PostgreSQL's partition-wise foreign key constraint management system