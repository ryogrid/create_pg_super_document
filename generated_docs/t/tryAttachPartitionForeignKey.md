# tryAttachPartitionForeignKey

## Location
src/backend/commands/tablecmds.c: 11071 - 11288

## Overview
tryAttachPartitionForeignKey attempts to attach an existing foreign key constraint in a partition to a parent constraint, avoiding the need to create duplicate constraints.

## Definition


## Detailed Description
This function compares an existing foreign key constraint in a partition (represented by ForeignKeyCacheInfo) with a parent constraint to determine if they are equivalent and can be linked. If they match, it establishes the parent-child relationship between the constraints and performs necessary cleanup.

The function performs a comprehensive comparison of constraint properties including:
- Referenced relation OID and number of key columns
- Column mappings (referencing and referenced columns)
- Operator equality functions
- Constraint attributes (deferrability, validation status, actions, etc.)

If the constraints are equivalent, the function:
1. Removes redundant action triggers from the partition (since parent triggers handle the partition)
2. Establishes the parent-child constraint relationship using ConstraintSetParentConstraint
3. Links partition check triggers to parent triggers using TriggerSetParentTrigger
4. If the referenced table is partitioned, removes extra constraint and trigger records that are no longer needed

## Parameters / Member Variables
- : ForeignKeyCacheInfo structure containing details of the partition's existing FK constraint
- : OID of the partition relation
- : OID of the parent constraint to potentially attach to
- : Number of foreign key columns
- : Array of referencing column numbers mapped to partition's column layout
- : Array of referenced column numbers
- : Array of equality operator OIDs for foreign key columns
- : OID of parent's insert trigger
- : OID of parent's update trigger  
- : Open relation handle for pg_trigger catalog

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Look up constraint tuples in system cache
  - [ConstraintSetParentConstraint](../C/ConstraintSetParentConstraint.md): Establish parent-child constraint relationship
  - [GetForeignKeyCheckTriggers](../G/GetForeignKeyCheckTriggers.md): Retrieve check trigger OIDs from partition constraint
  - [TriggerSetParentTrigger](../T/TriggerSetParentTrigger.md): Link partition triggers to parent triggers
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)/deleteDependencyRecordsForSpecific: Remove dependency records
  - [performDeletion](../p/performDeletion.md)/performMultipleDeletions: Delete redundant triggers and constraints
  - [get_rel_relkind](../g/get_rel_relkind.md): Check if referenced table is partitioned
  - CommandCounterIncrement: Make catalog changes visible

- Called from:
  - [CloneFkReferencing](../C/CloneFkReferencing.md): During partition FK constraint cloning process
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md): During recursive FK constraint processing

## Notes and Other Information
- Returns true if attachment successful, false if constraints are not compatible
- Performs extensive validation to ensure constraints are truly equivalent before attachment
- Handles cleanup of redundant triggers and constraints automatically
- Critical optimization that avoids creating duplicate FK constraints during partition operations
- Includes special handling for partitioned referenced tables to clean up extra constraint records
- Uses multiple CommandCounterIncrement calls to ensure catalog visibility during multi-step operations