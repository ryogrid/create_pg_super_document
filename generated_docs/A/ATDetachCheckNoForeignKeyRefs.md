# ATDetachCheckNoForeignKeyRefs

## Location
src/backend/commands/tablecmds.c: 20181 - 20229

## Overview
Verifies that foreign keys pointing to a partitioned table will not become invalid during a DETACH PARTITION operation, raising an error if any referenced values exist.

## Definition


## Detailed Description
This function is called during the DETACH PARTITION operation to ensure referential integrity. It checks all foreign key constraints that reference the partition being detached to verify that no foreign key references would become invalid after the partition is detached. The function retrieves all parented foreign key references to the partition and for each constraint, it performs a referential integrity check using the RI_PartitionRemove_Check function. If any referenced values exist that would become orphaned after detaching the partition, an error is raised to prevent the operation.

## Parameters / Member Variables
- : The relation (partition) that is being detached from its parent table

## Dependencies
- Functions called/Symbols referenced:
  - [GetParentedForeignKeyRefs](../G/GetParentedForeignKeyRefs.md)
  - Form_pg_constraint
  - Trigger
  - ShareLock
  - TRIGGER_FIRES_ON_ORIGIN
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)
- Called from (representative examples):
  - [ATExecDetachPartition](ATExecDetachPartition.md)

## Notes and Other Information
- This is a static function within tablecmds.c, used exclusively during partition detachment operations
- The function holds a ShareLock on the referencing table to prevent data changes until commit
- It creates a temporary trigger structure to pass to RI_PartitionRemove_Check for validation
- This check is essential for maintaining referential integrity when detaching partitions that are referenced by foreign keys