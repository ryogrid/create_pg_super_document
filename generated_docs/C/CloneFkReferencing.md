# CloneFkReferencing

## Location
src/backend/commands/tablecmds.c: 10830 - 11070

## Overview
CloneFkReferencing handles cloning foreign key constraints where the parent relation appears on the referencing side, attempting to reuse existing constraints before creating new ones.

## Definition


## Detailed Description
This function is a subroutine for CloneForeignKeyConstraints that manages foreign key constraints where the parent relation is on the referencing (source) side. For each FK constraint of the parent relation, it either finds an equivalent constraint in the partition that can be reparented, or creates a new constraint as a child of the parent constraint.

The function performs several key operations:
1. Validates that the partition is not already referenced by the parent (preventing circular references)
2. Checks that foreign tables don't have FK constraints (not supported)
3. For each parent FK constraint:
   - Maps column attributes between parent and partition
   - Attempts to attach existing compatible FK constraints using tryAttachPartitionForeignKey
   - If no compatible constraint exists, creates a new FK constraint
   - Handles trigger creation and recursive processing for sub-partitions

The function includes an optimization to avoid duplicate constraints by first trying to attach existing partition constraints to the parent constraint hierarchy rather than always creating new ones.

## Parameters / Member Variables
- : Optional work queue for phase-3 verification setup (can be NULL if verification not needed)
- : The parent relation that has the foreign key constraints to be cloned
- : The partition relation where constraints will be cloned or attached

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetFKeyList: Get list of FK constraints for a relation
  - build_attrmap_by_name: Map attributes between parent and partition
  - copyObject: Deep copy the partition's FK list
  - DeconstructFkConstraintRow: Extract FK constraint details from catalog tuple
  - GetForeignKeyCheckTriggers: Retrieve check trigger OIDs for constraint
  - tryAttachPartitionForeignKey: Attempt to attach existing FK constraint to parent
  - addFkConstraint: Create new FK constraint on partition
  - addFkRecurseReferencing: Recursively handle sub-partitions
  - find_all_inheritors: Lock all partitions of referenced partitioned table
  - get_constraint_name: Get constraint name for error reporting

- Called from:
  - CloneForeignKeyConstraints: Main entry point for cloning FK constraints during partition operations

## Notes and Other Information
- Includes protection against circular FK relationships by preventing attachment of tables that the parent already references
- Foreign tables cannot have FK constraints and will cause an error if attempted
- Uses an optimization strategy: tries to reuse existing compatible constraints before creating new ones
- Requires careful locking of referenced relations, especially when they are partitioned tables
- The wqueue parameter enables deferred constraint validation during ATTACH PARTITION operations
- Part of PostgreSQL's comprehensive partition-wise foreign key constraint management system